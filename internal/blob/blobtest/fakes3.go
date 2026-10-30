// Package blobtest provides an in-process S3 stand-in for tests.
package blobtest

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// FakeS3 is a minimal path-style S3 server supporting bucket create, object put and ranged get.
type FakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func NewFakeS3() *FakeS3 { return &FakeS3{objects: map[string][]byte{}} }

func (f *FakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/")
	switch {
	case r.Method == http.MethodPut && !strings.Contains(path, "/"):
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[path] = body
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet:
		data, ok := f.objects[path]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			return
		}
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			start, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-"))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start:])
			return
		}
		_, _ = w.Write(data)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Put seeds an object directly, bypassing HTTP.
func (f *FakeS3) Put(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = data
}
