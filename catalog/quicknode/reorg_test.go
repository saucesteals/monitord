package quicknode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
