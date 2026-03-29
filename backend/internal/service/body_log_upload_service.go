package service

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

const (
	bodyLogUploadLeaderKey = "bodylog:upload:leader"
	bodyLogUploadLeaderTTL = 30 * time.Minute
)

var bodyLogUploadCronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

var bodyLogUploadReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// BodyLogUploadService periodically compresses completed NDJSON files and
// uploads them to S3, reusing the backup S3 configuration.
type BodyLogUploadService struct {
	writer       *BodyLogWriter
	backupSvc    *BackupService
	cfg          *config.BodyLogConfig
	timezone     string
	redisClient  *redis.Client
	instanceID   string
	cron         *cron.Cron
	startOnce    sync.Once
	stopOnce     sync.Once
	warnOnce     sync.Once
}

func NewBodyLogUploadService(
	writer *BodyLogWriter,
	backupSvc *BackupService,
	cfg *config.BodyLogConfig,
	timezone string,
	redisClient *redis.Client,
) *BodyLogUploadService {
	return &BodyLogUploadService{
		writer:      writer,
		backupSvc:   backupSvc,
		cfg:         cfg,
		timezone:    timezone,
		redisClient: redisClient,
		instanceID:  uuid.NewString(),
	}
}

func (s *BodyLogUploadService) Start() {
	if s == nil || s.writer == nil || s.backupSvc == nil {
		return
	}
	if s.cfg == nil || !s.cfg.Enabled {
		return
	}

	s.startOnce.Do(func() {
		schedule := strings.TrimSpace(s.cfg.UploadSchedule)
		if schedule == "" {
			schedule = "7 * * * *"
		}

		loc := time.Local
		if tz := strings.TrimSpace(s.timezone); tz != "" {
			if parsed, err := time.LoadLocation(tz); err == nil && parsed != nil {
				loc = parsed
			}
		}

		c := cron.New(cron.WithParser(bodyLogUploadCronParser), cron.WithLocation(loc))
		_, err := c.AddFunc(schedule, func() { s.runScheduled() })
		if err != nil {
			logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] invalid schedule=%q: %v", schedule, err)
			return
		}
		s.cron = c
		s.cron.Start()
		logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] started (schedule=%q tz=%s)", schedule, loc.String())
	})
}

func (s *BodyLogUploadService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.cron != nil {
			ctx := s.cron.Stop()
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
				logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] cron stop timed out")
			}
		}
	})
}

func (s *BodyLogUploadService) runScheduled() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	release, ok := s.tryAcquireLeaderLock(ctx)
	if !ok {
		return
	}
	if release != nil {
		defer release()
	}

	files, err := s.writer.RotateAndListCompleted()
	if err != nil {
		logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] list completed files failed: %v", err)
		return
	}
	if len(files) == 0 {
		return
	}

	store, err := s.backupSvc.GetObjectStore(ctx)
	if err != nil {
		logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] get S3 store failed: %v", err)
		return
	}

	var uploaded, failed int
	for _, path := range files {
		if err := s.uploadAndDelete(ctx, store, path); err != nil {
			logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] upload %s failed: %v", filepath.Base(path), err)
			failed++
		} else {
			uploaded++
		}
	}

	// Cleanup old local files that may have failed to upload previously.
	s.cleanupOldLocalFiles()

	if uploaded > 0 || failed > 0 {
		logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] done: uploaded=%d failed=%d", uploaded, failed)
	}
}

func (s *BodyLogUploadService) uploadAndDelete(ctx context.Context, store BackupObjectStore, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	// Build S3 key: <prefix>/<YYYY-MM-DD>/<filename>.gz
	prefix := strings.TrimRight(s.cfg.S3Prefix, "/")
	if prefix == "" {
		prefix = "body_logs"
	}
	base := filepath.Base(localPath)
	dateDir := time.Now().UTC().Format("2006-01-02")
	key := fmt.Sprintf("%s/%s/%s.gz", prefix, dateDir, base)

	pr, pw := io.Pipe()
	go func() {
		gz := gzip.NewWriter(pw)
		_, copyErr := io.Copy(gz, f)
		closeErr := gz.Close()
		if copyErr != nil {
			_ = pw.CloseWithError(copyErr)
		} else if closeErr != nil {
			_ = pw.CloseWithError(closeErr)
		} else {
			_ = pw.Close()
		}
	}()

	_, err = store.Upload(ctx, key, pr, "application/gzip")
	if err != nil {
		return fmt.Errorf("upload s3 key=%s: %w", key, err)
	}

	if err := os.Remove(localPath); err != nil {
		logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] remove local %s failed: %v", base, err)
	}
	return nil
}

func (s *BodyLogUploadService) cleanupOldLocalFiles() {
	retainHours := s.cfg.RetainLocalHours
	if retainHours <= 0 {
		retainHours = 24
	}
	cutoff := time.Now().Add(-time.Duration(retainHours) * time.Hour)

	entries, err := os.ReadDir(s.writer.Dir())
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".ndjson") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(s.writer.Dir(), e.Name()))
		}
	}
}

func (s *BodyLogUploadService) tryAcquireLeaderLock(ctx context.Context) (func(), bool) {
	if s.redisClient != nil {
		ok, err := s.redisClient.SetNX(ctx, bodyLogUploadLeaderKey, s.instanceID, bodyLogUploadLeaderTTL).Result()
		if err == nil {
			if !ok {
				return nil, false
			}
			return func() {
				_, _ = bodyLogUploadReleaseScript.Run(ctx, s.redisClient, []string{bodyLogUploadLeaderKey}, s.instanceID).Result()
			}, true
		}
		s.warnOnce.Do(func() {
			logger.LegacyPrintf("service.body_log_upload", "[BodyLogUpload] Redis leader lock failed, running without lock: %v", err)
		})
	}
	// No Redis or Redis error: run anyway (single instance assumption).
	return nil, true
}
