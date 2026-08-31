package quicknode

import "testing"

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
