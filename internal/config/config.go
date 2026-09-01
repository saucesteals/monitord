// Package config resolves the monitord state layout and the local config file.
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/saucesteals/monitord/internal/model"
)

// Paths is the resolved monitord state layout.
type Paths struct {
	Root string `json:"root"`
	// MonitorsDir holds authored monitor sources, one directory per monitor.
	// It is a single Go module so monitors can share dependency versions.
	MonitorsDir string `json:"monitors_dir"`
	// ArtifactsDir is a global content-addressed build cache. Database artifact
	// rows are created only when a deployment commits successfully.
	ArtifactsDir string `json:"artifacts_dir"`
	// LibDir is the monitord checkout monitors link against. Keeping a clone
	// inside the root makes an install self-contained: monitors build against
	// the same library as the daemon running them, not whatever happens to be
	// in a developer's working tree.
	LibDir string `json:"lib_dir"`
	// BinDir holds the installed monitord binary.
	BinDir     string `json:"bin_dir"`
	StateDir   string `json:"state_dir"`
	LogsDir    string `json:"logs_dir"`
	DBPath     string `json:"db_path"`
	ConfigPath string `json:"config_path"`
}

// MonitorDir returns the authored source directory for a monitor.
func (p Paths) MonitorDir(name model.MonitorName) string {
	return filepath.Join(p.MonitorsDir, name.String())
}

// ModulePath is the go.mod shared by every monitor.
func (p Paths) ModulePath() string {
	return filepath.Join(p.MonitorsDir, "go.mod")
}

// Config is the on-disk daemon configuration.
//
// Proxies deliberately do not live here. They are a resource monitord stores
// and owns, so importing more never means editing config or restarting.
type Config struct {
	Paths
}

// Resolve returns the filesystem paths used by monitord.
func Resolve(root string) (Paths, error) {
	if root == "" {
		root = os.Getenv("MONITORD_ROOT")
	}
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Paths{}, fmt.Errorf("resolve home dir: %w", err)
		}
		root = filepath.Join(home, ".monitord")
	}

	root, err := filepath.Abs(root)
	if err != nil {
		return Paths{}, fmt.Errorf("resolve root: %w", err)
	}
	stateDir := filepath.Join(root, "state")

	return Paths{
		Root:         root,
		MonitorsDir:  filepath.Join(root, "monitors"),
		ArtifactsDir: filepath.Join(root, "artifacts"),
		LibDir:       filepath.Join(root, "lib"),
		BinDir:       filepath.Join(root, "bin"),
		StateDir:     stateDir,
		LogsDir:      filepath.Join(root, "logs"),
		DBPath:       filepath.Join(stateDir, "monitord.db"),
		ConfigPath:   filepath.Join(root, "config.json"),
	}, nil
}

// Init creates the monitord root layout and config file if absent.
func Init(root string) (Paths, error) {
	paths, err := Resolve(root)
	if err != nil {
		return Paths{}, err
	}

	for _, dir := range []string{paths.Root, paths.MonitorsDir, paths.ArtifactsDir, paths.BinDir, paths.StateDir, paths.LogsDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return Paths{}, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	if err := ensureModule(paths); err != nil {
		return Paths{}, err
	}

	switch _, err := os.Stat(paths.ConfigPath); {
	case err == nil:
		return paths, nil
	case !errors.Is(err, os.ErrNotExist):
		return Paths{}, fmt.Errorf("stat config: %w", err)
	}

	if err := Save(Config{Paths: paths}); err != nil {
		return Paths{}, err
	}

	return paths, nil
}

const (
	// ModuleName is the Go module every monitor belongs to. Monitors are
	// packages inside it, so they share one dependency set and one tidy.
	ModuleName = "monitord.local/monitors"
	// SDKModule is the import path monitors use for the authoring SDK.
	SDKModule = "github.com/saucesteals/monitord"
)

// SDKPath locates the monitord checkout the monitors module links against.
//
// Resolution order: an explicit MONITORD_SDK_PATH, then the clone inside the
// root. A normal install uses the clone, which keeps the daemon and monitor SDK
// pinned together. Development roots can point MONITORD_SDK_PATH at a checkout.
func (p Paths) SDKPath() string {
	candidates := []string{
		os.Getenv("MONITORD_SDK_PATH"),
		p.LibDir,
	}

	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err == nil {
			return candidate
		}
	}

	return ""
}

// ensureModule keeps the shared monitors go.mod present and pointed at the
// right SDK checkout, so a scaffolded monitor builds without go.mod editing and
// a relocated install repoints itself instead of building against a stale path.
func ensureModule(paths Paths) error {
	sdk := paths.SDKPath()

	switch _, err := os.Stat(paths.ModulePath()); {
	case errors.Is(err, os.ErrNotExist):
		contents := fmt.Sprintf("module %s\n\ngo 1.24\n\nrequire %s v0.0.0\n", ModuleName, SDKModule)
		if err := os.WriteFile(paths.ModulePath(), []byte(contents), 0o600); err != nil {
			return fmt.Errorf("create monitors go.mod: %w", err)
		}
	case err != nil:
		return fmt.Errorf("stat monitors go.mod: %w", err)
	}

	if sdk == "" {
		return nil
	}

	return setSDKReplace(paths, sdk)
}

// setSDKReplace points the monitors module at the given SDK checkout, rewriting
// an existing replace directive if it has drifted.
func setSDKReplace(paths Paths, sdk string) error {
	current, err := os.ReadFile(paths.ModulePath())
	if err != nil {
		return fmt.Errorf("read monitors go.mod: %w", err)
	}
	if strings.Contains(string(current), fmt.Sprintf("=> %s", sdk)) {
		return nil
	}

	// go mod edit separates old from new with "=", not the "=>" used in go.mod.
	cmd := exec.Command("go", "mod", "edit", "-replace="+SDKModule+"="+sdk)
	cmd.Dir = paths.MonitorsDir
	cmd.Env = append(os.Environ(), "GOWORK=off")

	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("point monitors module at %s: %w\n%s", sdk, err, strings.TrimSpace(stderr.String()))
	}

	return nil
}

// Tidy resolves monitor dependencies across the shared monitors module.
func Tidy(ctx context.Context, paths Paths) error {
	cmd := exec.CommandContext(ctx, "go", "mod", "tidy")
	cmd.Dir = paths.MonitorsDir
	cmd.Env = append(os.Environ(), "GOWORK=off")

	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go mod tidy in %s: %w\n%s", paths.MonitorsDir, err, strings.TrimSpace(stderr.String()))
	}

	return nil
}

// Load reads config.json, creating the root layout first when needed.
func Load(root string) (Config, error) {
	paths, err := Init(root)
	if err != nil {
		return Config{}, err
	}

	file, err := os.Open(paths.ConfigPath)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = file.Close() }()

	cfg := Config{Paths: paths}
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}

	// Paths always come from the caller's root, never from the file, so a
	// relocated root cannot be shadowed by stale values.
	cfg.Paths = paths

	return cfg, nil
}

// Save writes config.json via a temp file and rename.
func Save(cfg Config) error {
	if cfg.ConfigPath == "" {
		return errors.New("config path is required")
	}

	tmp := cfg.ConfigPath + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create config temp file: %w", err)
	}

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	encErr := enc.Encode(cfg)
	closeErr := file.Close()
	if encErr != nil {
		return fmt.Errorf("write config: %w", encErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close config: %w", closeErr)
	}

	if err := os.Rename(tmp, cfg.ConfigPath); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}

	return nil
}
