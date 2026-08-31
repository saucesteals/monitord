package quicknode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOpenHandshakeRetriesAndRedacts(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type=%q", got)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "0x1"})
	}))
	defer s.Close()
	c, err := Open(context.Background(), Config{HTTPURL: s.URL + "/private-token?secret=yes"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.ChainID() != "0x1" {
		t.Fatalf("chain=%q", c.ChainID())
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	redacted := RedactEndpoint(s.URL + "/private-token?secret=yes")
	if strings.Contains(redacted, "private-token") || strings.Contains(redacted, "secret") {
		t.Fatalf("not redacted: %s", redacted)
	}
}

func TestOpenRejectsMalformedHandshakeWithoutLeakingEndpoint(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer s.Close()
	secretURL := s.URL + "/my-secret-token"
	_, err := Open(context.Background(), Config{HTTPURL: secretURL})
	if err == nil {
		t.Fatal("Open succeeded")
	}
	if strings.Contains(err.Error(), "my-secret-token") {
		t.Fatalf("error leaked URL: %v", err)
	}
}
