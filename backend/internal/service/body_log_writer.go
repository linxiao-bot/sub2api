package service

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// BodyLogEntry represents a single request/response pair to be persisted.
type BodyLogEntry struct {
	RequestID     string          `json:"request_id"`
	Timestamp     time.Time       `json:"ts"`
	UserID        int64           `json:"user_id,omitempty"`
	APIKeyID      int64           `json:"api_key_id,omitempty"`
	AccountID     int64           `json:"account_id,omitempty"`
	GroupID       *int64          `json:"group_id,omitempty"`
	Model         string          `json:"model"`
	UpstreamModel string          `json:"upstream_model,omitempty"`
	Stream        bool            `json:"stream"`
	Platform      string          `json:"platform,omitempty"`
	DurationMs    int64           `json:"duration_ms"`
	RequestBody   json.RawMessage `json:"request_body"`
	ResponseBody  json.RawMessage `json:"response_body"`
	RequestPath   string          `json:"request_path,omitempty"`
	ClientIP      string          `json:"client_ip,omitempty"`
}

const (
	bodyLogBatchSize   = 32
	bodyLogBatchWindow = 200 * time.Millisecond
	bodyLogDrainTimeout = 10 * time.Second
)

// BodyLogWriter writes BodyLogEntry records as NDJSON to local rotating files.
type BodyLogWriter struct {
	cfg      *config.BodyLogConfig
	dir      string
	hostname string

	queue chan *BodyLogEntry
	wg    sync.WaitGroup

	mu       sync.Mutex // protects file, fileSize
	file     *os.File
	fileSize int64

	stopped  atomic.Bool
	stopOnce sync.Once

	// metrics
	enqueued atomic.Int64
	dropped  atomic.Int64
	written  atomic.Int64
}

func NewBodyLogWriter(cfg *config.BodyLogConfig) *BodyLogWriter {
	dir := strings.TrimSpace(cfg.LocalDir)
	if dir == "" {
		dataDir := os.Getenv("DATA_DIR")
		if dataDir == "" {
			// Match setup.GetDataDir() fallback: prefer /app/data if writable.
			if info, err := os.Stat("/app/data"); err == nil && info.IsDir() {
				dataDir = "/app/data"
			} else {
				dataDir = "."
			}
		}
		dir = filepath.Join(dataDir, "body_logs")
	}
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	return &BodyLogWriter{
		cfg:      cfg,
		dir:      dir,
		hostname: hostname,
		queue:    make(chan *BodyLogEntry, cfg.QueueSize),
	}
}

// Start launches the writer worker goroutines.
func (w *BodyLogWriter) Start() {
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		log.Printf("[BodyLog] failed to create dir %s: %v", w.dir, err)
	}
	workers := w.cfg.WorkerCount
	if workers <= 0 {
		workers = 4
	}
	w.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go w.worker()
	}
	log.Printf("[BodyLog] started %d workers, queue=%d, dir=%s", workers, w.cfg.QueueSize, w.dir)
}

// Stop gracefully shuts down: closes the queue channel, waits for workers to
// drain remaining entries, and closes the active file.
func (w *BodyLogWriter) Stop() {
	w.stopOnce.Do(func() {
		w.stopped.Store(true)
		close(w.queue)

		done := make(chan struct{})
		go func() {
			w.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(bodyLogDrainTimeout):
			log.Printf("[BodyLog] drain timed out after %s", bodyLogDrainTimeout)
		}

		w.mu.Lock()
		if w.file != nil {
			_ = w.file.Close()
			w.file = nil
		}
		w.mu.Unlock()
		log.Printf("[BodyLog] stopped (written=%d enqueued=%d dropped=%d)",
			w.written.Load(), w.enqueued.Load(), w.dropped.Load())
	})
}

// Enqueue adds an entry to the write queue. Non-blocking: drops if full.
func (w *BodyLogWriter) Enqueue(entry *BodyLogEntry) {
	if entry == nil || w.stopped.Load() {
		return
	}
	select {
	case w.queue <- entry:
		w.enqueued.Add(1)
	default:
		w.dropped.Add(1)
	}
}

// RotateAndListCompleted forces a rotation of the current file and returns
// paths of all completed (non-current) NDJSON files in the directory.
func (w *BodyLogWriter) RotateAndListCompleted() ([]string, error) {
	w.mu.Lock()
	w.rotateFileLocked()
	w.mu.Unlock()

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("readdir %s: %w", w.dir, err)
	}

	currentName := w.currentFileName()
	var completed []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == currentName {
			continue
		}
		if strings.HasSuffix(name, ".ndjson") {
			completed = append(completed, filepath.Join(w.dir, name))
		}
	}
	return completed, nil
}

// Dir returns the configured local directory.
func (w *BodyLogWriter) Dir() string { return w.dir }

// Stats returns current counters.
func (w *BodyLogWriter) Stats() (enqueued, dropped, written int64) {
	return w.enqueued.Load(), w.dropped.Load(), w.written.Load()
}

func (w *BodyLogWriter) worker() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[BodyLog] worker panic: %v\n%s", r, debug.Stack())
		}
	}()

	for {
		entry, ok := <-w.queue
		if !ok {
			return
		}

		batch := make([]*BodyLogEntry, 0, bodyLogBatchSize)
		batch = append(batch, entry)

		timer := time.NewTimer(bodyLogBatchWindow)
	batchLoop:
		for len(batch) < bodyLogBatchSize {
			select {
			case next, ok := <-w.queue:
				if !ok {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					w.flushBatch(batch)
					return
				}
				batch = append(batch, next)
			case <-timer.C:
				break batchLoop
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		w.flushBatch(batch)
	}
}

func (w *BodyLogWriter) flushBatch(batch []*BodyLogEntry) {
	if len(batch) == 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.ensureFileLocked(); err != nil {
		log.Printf("[BodyLog] open file failed: %v", err)
		return
	}

	var n int64
	for _, entry := range batch {
		data, err := json.Marshal(entry)
		if err != nil {
			log.Printf("[BodyLog] marshal failed: %v", err)
			continue
		}
		data = append(data, '\n')
		written, err := w.file.Write(data)
		if err != nil {
			log.Printf("[BodyLog] write failed: %v", err)
			continue
		}
		n++
		w.fileSize += int64(written)
	}
	w.written.Add(n)

	maxBytes := int64(w.cfg.MaxFileSizeMB) * 1024 * 1024
	if maxBytes > 0 && w.fileSize >= maxBytes {
		w.rotateFileLocked()
	}
}

func (w *BodyLogWriter) currentFileName() string {
	return fmt.Sprintf("body_log_%s_current.ndjson", w.hostname)
}

func (w *BodyLogWriter) ensureFileLocked() error {
	if w.file != nil {
		return nil
	}
	path := filepath.Join(w.dir, w.currentFileName())
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.file = f
	w.fileSize = info.Size()
	return nil
}

// rotateFileLocked closes the active file and renames it with a timestamp.
// Caller must hold w.mu.
func (w *BodyLogWriter) rotateFileLocked() {
	if w.file == nil {
		return
	}
	_ = w.file.Close()
	w.file = nil
	w.fileSize = 0

	oldPath := filepath.Join(w.dir, w.currentFileName())
	if info, err := os.Stat(oldPath); err != nil || info.Size() == 0 {
		// Nothing to rotate or file doesn't exist.
		if err == nil && info.Size() == 0 {
			_ = os.Remove(oldPath)
		}
		return
	}
	ts := time.Now().UTC().Format("20060102T150405Z")
	newName := fmt.Sprintf("body_log_%s_%s.ndjson", w.hostname, ts)
	newPath := filepath.Join(w.dir, newName)
	if err := os.Rename(oldPath, newPath); err != nil {
		log.Printf("[BodyLog] rotate rename failed: %v", err)
	}
}
