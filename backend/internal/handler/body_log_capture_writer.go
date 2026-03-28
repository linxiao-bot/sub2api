package handler

import (
	"bytes"
	"sync"

	"github.com/gin-gonic/gin"
)

// bodyLogCaptureWriter wraps a gin.ResponseWriter and tee-copies all written
// bytes into an internal buffer for body-log recording. Unlike opsCaptureWriter
// it captures ALL status codes (not just errors) but respects a configurable
// maxBytes safety valve to prevent OOM on very large responses.
type bodyLogCaptureWriter struct {
	gin.ResponseWriter
	maxBytes int
	buf      bytes.Buffer
	overflow bool
}

var bodyLogCaptureWriterPool = sync.Pool{
	New: func() any {
		return &bodyLogCaptureWriter{}
	},
}

func acquireBodyLogCaptureWriter(rw gin.ResponseWriter, maxBytes int) *bodyLogCaptureWriter {
	w, ok := bodyLogCaptureWriterPool.Get().(*bodyLogCaptureWriter)
	if !ok || w == nil {
		w = &bodyLogCaptureWriter{}
	}
	w.ResponseWriter = rw
	w.maxBytes = maxBytes
	w.buf.Reset()
	w.overflow = false
	return w
}

func releaseBodyLogCaptureWriter(w *bodyLogCaptureWriter) {
	if w == nil {
		return
	}
	w.ResponseWriter = nil
	w.buf.Reset()
	w.overflow = false
	bodyLogCaptureWriterPool.Put(w)
}

func (w *bodyLogCaptureWriter) Write(b []byte) (int, error) {
	if !w.overflow {
		if w.buf.Len()+len(b) > w.maxBytes {
			w.overflow = true
			w.buf.Reset()
		} else {
			_, _ = w.buf.Write(b)
		}
	}
	return w.ResponseWriter.Write(b)
}

func (w *bodyLogCaptureWriter) WriteString(s string) (int, error) {
	if !w.overflow {
		if w.buf.Len()+len(s) > w.maxBytes {
			w.overflow = true
			w.buf.Reset()
		} else {
			_, _ = w.buf.WriteString(s)
		}
	}
	return w.ResponseWriter.WriteString(s)
}

// CapturedBody returns the captured response bytes. Returns nil if the
// response overflowed the safety limit or nothing was written.
func (w *bodyLogCaptureWriter) CapturedBody() []byte {
	if w.overflow || w.buf.Len() == 0 {
		return nil
	}
	return w.buf.Bytes()
}
