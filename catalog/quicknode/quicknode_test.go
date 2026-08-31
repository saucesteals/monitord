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

func TestIdentifiersAndCanonicalQuantities(t *testing.T) {
	a, err := ParseAddress("0xAABBccDDeeFF0011223344556677889900AaBbCc")
	if err != nil || a != "0xaabbccddeeff0011223344556677889900aabbcc" {
		t.Fatalf("ParseAddress() = %q, %v", a, err)
	}
	if _, err = ParseAddress("0x1234"); err == nil {
		t.Fatal("short address accepted")
	}
	if _, err = ParseChainID("0x01"); err == nil {
		t.Fatal("noncanonical chain ID accepted")
	}
	if got, err := ParseChainID("0xA"); err != nil || got != "0xa" {
		t.Fatalf("ParseChainID()=%q,%v", got, err)
	}
	index := uint(7)
	tr := Transfer{ChainID: "0x1", Kind: ERC20, BlockHash: Hash("0x" + strings.Repeat("1", 64)), TxHash: Hash("0x" + strings.Repeat("2", 64)), LogIndex: &index}
	if got, want := tr.ID(), "evm:0x1:log:0x"+strings.Repeat("1", 64)+":0x"+strings.Repeat("2", 64)+":7"; got != want {
		t.Fatalf("ID=%q, want %q", got, want)
	}
}

func TestLogCloneOwnsBuffers(t *testing.T) {
	l := Log{Topics: []Hash{"a"}, Data: []byte{1}}
	c := l.Clone()
	l.Topics[0] = "b"
	l.Data[0] = 2
	if c.Topics[0] != "a" || c.Data[0] != 1 {
		t.Fatal("clone aliases source")
	}
}

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

func TestDecodeTransferLog(t *testing.T) {
	wallet := Address("0x1111111111111111111111111111111111111111")
	from := Hash("0x" + strings.Repeat("0", 24) + strings.Repeat("1", 40))
	to := Hash("0x" + strings.Repeat("0", 24) + strings.Repeat("2", 40))
	idx := uint(3)
	l := Log{BlockNumber: 9, BlockHash: Hash("0x" + strings.Repeat("a", 64)), TxHash: Hash("0x" + strings.Repeat("b", 64)), LogIndex: idx, Address: Address("0x3333333333333333333333333333333333333333"), Topics: []Hash{transferTopic, from, to}, Data: []byte{0x2a}}
	transfers, err := decodeTransferLog("0x1", l, wallet)
	if err != nil || len(transfers) != 1 {
		t.Fatalf("decode=%+v,%v", transfers, err)
	}
	tr := transfers[0]
	if tr.Kind != ERC20 || tr.Amount != "42" || tr.From != wallet || tr.ID() == "" {
		t.Fatalf("transfer=%+v", tr)
	}
}

func TestDecodeERC1155SingleAndBatch(t *testing.T) {
	wallet := Address("0x1111111111111111111111111111111111111111")
	zeroTopic := strings.Repeat("0", 24)
	from := Hash("0x" + zeroTopic + strings.Repeat("1", 40))
	to := Hash("0x" + zeroTopic + strings.Repeat("2", 40))
	operator := Hash("0x" + zeroTopic + strings.Repeat("3", 40))
	base := Log{BlockHash: Hash("0x" + strings.Repeat("a", 64)), TxHash: Hash("0x" + strings.Repeat("b", 64)), Address: Address("0x4444444444444444444444444444444444444444")}
	word := func(n byte) []byte { b := make([]byte, 32); b[31] = n; return b }
	single := base
	single.Topics = []Hash{transferSingleTopic, operator, from, to}
	single.Data = append(word(7), word(9)...)
	got, err := decodeTransferLog("0x1", single, wallet)
	if err != nil || len(got) != 1 || got[0].Kind != ERC1155 || got[0].TokenID != "7" || got[0].Amount != "9" {
		t.Fatalf("single=%+v,%v", got, err)
	}
	batch := base
	batch.Topics = []Hash{transferBatchTopic, operator, from, to}
	batch.Data = append(batch.Data, word(64)...)
	batch.Data = append(batch.Data, word(160)...)
	batch.Data = append(batch.Data, word(2)...)
	batch.Data = append(batch.Data, word(7)...)
	batch.Data = append(batch.Data, word(8)...)
	batch.Data = append(batch.Data, word(2)...)
	batch.Data = append(batch.Data, word(9)...)
	batch.Data = append(batch.Data, word(10)...)
	got, err = decodeTransferLog("0x1", batch, wallet)
	if err != nil || len(got) != 2 || got[1].TokenID != "8" || got[1].Amount != "10" || got[0].ID() == got[1].ID() {
		t.Fatalf("batch=%+v,%v", got, err)
	}
}

func TestConfirmationPolicyAndWSSDerivation(t *testing.T) {
	if n, err := confirmationDepth("0x1", 0); err != nil || n != 12 {
		t.Fatalf("mainnet=%d,%v", n, err)
	}
	if _, err := confirmationDepth("0x89", 0); err == nil {
		t.Fatal("unknown chain got default")
	}
	got, err := HTTPFromWSS("wss://example.quiknode.pro/token")
	if err != nil || got != "https://example.quiknode.pro/token" {
		t.Fatalf("derived=%q,%v", got, err)
	}
	if _, err = HTTPFromWSS("https://example.com/token"); err == nil {
		t.Fatal("accepted HTTP input")
	}
}

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

func TestDeepReorgFindsAncestorAndBuildsCorrections(t *testing.T) {
	hashes := map[string]string{"0x1": "0x" + strings.Repeat("a", 64), "0x2": "0x" + strings.Repeat("c", 64)}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		result := any("0x1")
		if req.Method == "eth_getBlockByNumber" {
			params := req.Params.([]any)
			result = map[string]any{"number": params[0], "hash": hashes[params[0].(string)], "parentHash": "0x" + strings.Repeat("0", 64)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	defer s.Close()
	c, err := Open(context.Background(), Config{HTTPURL: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	tr := Transfer{ChainID: "0x1", Kind: Native, BlockHash: Hash("0x" + strings.Repeat("b", 64)), TxHash: Hash("0x" + strings.Repeat("d", 64)), Amount: "1"}
	j := walletJournal{Blocks: []walletJournalBlock{{Number: 1, Hash: Hash(hashes["0x1"])}, {Number: 2, Hash: Hash("0x" + strings.Repeat("b", 64)), Transfers: []Transfer{tr}}}}
	rewind, orphaned, err := reconcileJournal(context.Background(), c, j, 3)
	if err != nil {
		t.Fatal(err)
	}
	if rewind != 2 || len(orphaned) != 1 || orphaned[0].ID() != tr.ID() {
		t.Fatalf("rewind=%d orphaned=%+v", rewind, orphaned)
	}
	ev := (Wallet{}).correctionEvent(orphaned[0])
	if ev.ID != "correction:"+tr.ID() || !strings.Contains(ev.Details, tr.ID()) {
		t.Fatalf("correction=%+v", ev)
	}
}
