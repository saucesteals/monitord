package quicknode

import (
	"strings"
	"testing"
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
