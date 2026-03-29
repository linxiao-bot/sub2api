package handler

import (
	"encoding/json"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// globalBodyLogWriter is set during application startup. If nil, body logging
// is disabled and all helper functions are no-ops.
var globalBodyLogWriter *service.BodyLogWriter

// globalBodyLogMaxCaptureBytes is the safety limit for response capture.
var globalBodyLogMaxCaptureBytes int

// InitBodyLog initialises the package-level body log state.
// Must be called once during server setup, before any handler serves traffic.
func InitBodyLog(writer *service.BodyLogWriter, cfg *config.BodyLogConfig) {
	globalBodyLogWriter = writer
	if cfg != nil && cfg.MaxCaptureSizeMB > 0 {
		globalBodyLogMaxCaptureBytes = cfg.MaxCaptureSizeMB * 1024 * 1024
	} else {
		globalBodyLogMaxCaptureBytes = 10 * 1024 * 1024
	}
}

// bodyLogInstallCapture wraps c.Writer with a capture writer if body logging
// is enabled. Returns the capture writer (may be nil) and a cleanup func.
func bodyLogInstallCapture(c *gin.Context) (*bodyLogCaptureWriter, func()) {
	if globalBodyLogWriter == nil {
		return nil, func() {}
	}
	capture := acquireBodyLogCaptureWriter(c.Writer, globalBodyLogMaxCaptureBytes)
	c.Writer = capture
	return capture, func() {
		c.Writer = capture.ResponseWriter
		releaseBodyLogCaptureWriter(capture)
	}
}

// bodyLogResultInfo holds the minimal fields extracted from different forward
// result types (ForwardResult, OpenAIForwardResult, etc.).
type bodyLogResultInfo struct {
	RequestID     string
	Model         string
	UpstreamModel string
	Stream        bool
	DurationMs    int64
}

func bodyLogResultFromForward(r *service.ForwardResult) *bodyLogResultInfo {
	if r == nil {
		return nil
	}
	return &bodyLogResultInfo{
		RequestID:     r.RequestID,
		Model:         r.Model,
		UpstreamModel: r.UpstreamModel,
		Stream:        r.Stream,
		DurationMs:    r.Duration.Milliseconds(),
	}
}

func bodyLogResultFromOpenAI(r *service.OpenAIForwardResult) *bodyLogResultInfo {
	if r == nil {
		return nil
	}
	return &bodyLogResultInfo{
		RequestID:     r.RequestID,
		Model:         r.Model,
		UpstreamModel: r.UpstreamModel,
		Stream:        r.Stream,
		DurationMs:    r.Duration.Milliseconds(),
	}
}

// bodyLogEnqueue submits a body-log entry if capture was successful.
func bodyLogEnqueue(
	capture *bodyLogCaptureWriter,
	requestBody []byte,
	result *bodyLogResultInfo,
	apiKey *service.APIKey,
	account *service.Account,
	platform string,
	requestPath string,
	clientIP string,
) {
	if globalBodyLogWriter == nil || capture == nil || result == nil {
		return
	}
	captured := capture.CapturedBody()
	if captured == nil {
		return
	}

	sanitizedReq := service.RedactSensitiveJSONBytes(requestBody)
	if !json.Valid(sanitizedReq) {
		sanitizedReq, _ = json.Marshal(string(sanitizedReq))
	}

	var respBody json.RawMessage
	if json.Valid(captured) {
		respBody = json.RawMessage(captured)
	} else {
		respBody, _ = json.Marshal(string(captured))
	}

	entry := &service.BodyLogEntry{
		RequestID:    result.RequestID,
		Timestamp:    time.Now(),
		Model:        result.Model,
		Stream:       result.Stream,
		Platform:     platform,
		DurationMs:   result.DurationMs,
		RequestBody:  sanitizedReq,
		ResponseBody: respBody,
		RequestPath:  requestPath,
		ClientIP:     clientIP,
	}
	if apiKey != nil {
		entry.APIKeyID = apiKey.ID
		entry.GroupID = apiKey.GroupID
		if apiKey.User != nil {
			entry.UserID = apiKey.User.ID
		}
	}
	if account != nil {
		entry.AccountID = account.ID
	}
	if result.UpstreamModel != "" && result.UpstreamModel != result.Model {
		entry.UpstreamModel = result.UpstreamModel
	}
	globalBodyLogWriter.Enqueue(entry)
}
