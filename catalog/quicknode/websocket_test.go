package quicknode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSubscriptionReconnectsAndErrorsRedactEndpoint(t *testing.T) {
	var connections atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_, payload, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var req rpcRequest
		if json.Unmarshal(payload, &req) != nil {
			return
		}
		if req.Method == "eth_chainId" {
			_ = wsWrite(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "0x1"})
			return
		}
		n := connections.Add(1)
		_ = wsWrite(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "sub"})
		if n == 1 {
			_ = conn.Close(websocket.StatusInternalError, "retry")
			return
		}
		_ = wsWrite(r.Context(), conn, map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"subscription": "sub", "result": map[string]any{"number": "0x2", "hash": "0x" + strings.Repeat("a", 64), "parentHash": "0x" + strings.Repeat("b", 64)}}})
		<-r.Context().Done()
	}))
	defer s.Close()
	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/private-token"
	c, err := Open(context.Background(), Config{WSSURL: wsURL})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.SubscribeHeads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case head := <-sub.C():
		if head.Number != "0x2" {
			t.Fatalf("head=%+v", head)
		}
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reconnect")
	}
	if connections.Load() < 2 {
		t.Fatalf("connections=%d", connections.Load())
	}
	_, err = Open(context.Background(), Config{WSSURL: "ws://127.0.0.1:1/private-secret"})
	if err == nil || strings.Contains(err.Error(), "private-secret") {
		t.Fatalf("unredacted dial error: %v", err)
	}
}
