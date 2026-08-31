package monitor

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigOwnsLifetimeAndDestinations(t *testing.T) {
	dir := t.TempDir()
	raw := []byte("ttl: 1h\ndeliveries:\n  - discord:\n      webhook_url: https://discord.com/api/webhooks/123456789012345678/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n")
	if err := os.WriteFile(filepath.Join(dir, ConfigFileName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TTL != time.Hour || len(cfg.Deliveries) != 1 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}

func TestConfigRejectsPlanFields(t *testing.T) {
	dir := t.TempDir()
	raw := []byte("ttl: 1h\nevery: 5m\ndeliveries:\n  - discord:\n      webhook_url: https://discord.com/api/webhooks/123456789012345678/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n")
	if err := os.WriteFile(filepath.Join(dir, ConfigFileName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(dir); err == nil {
		t.Fatal("expected authored plan field to be rejected")
	}
}
