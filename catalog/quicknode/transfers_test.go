package quicknode

import (
	"strings"
	"testing"
)

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
