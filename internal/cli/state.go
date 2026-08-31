package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/spf13/cobra"
)

func (c *CLI) newStateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state",
		Short: "Manage monitor state",
		RunE: func(_ *cobra.Command, _ []string) error {
			return requireSubcommand("state", "get, set, clear")
		},
	}
	cmd.AddCommand(
		c.newStateGetCmd(),
		c.newStateSetCmd(),
		c.newStateClearCmd(),
	)

	return cmd
}

func (c *CLI) newStateGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get NAME",
		Short: "Print stored monitor state as JSON",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.stateGet(args[0])
		},
	}
}

func (c *CLI) stateGet(selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	m, err := store.GetDeployment(context.Background(), selector)
	if err != nil {
		return err
	}

	fmt.Println(indentJSON(m.State))

	return nil
}

// stateSet replaces stored state from a file, or from stdin when given "-".
//
// The write bumps the state revision, so a callback already in flight loses to this
// edit instead of overwriting it.
func (c *CLI) newStateSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set NAME FILE",
		Short: "Replace stored monitor state",
		Args:  exactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.stateSet(args[0], args[1])
		},
	}
}

func (c *CLI) stateSet(selector string, source string) error {
	raw, err := readStateFile(source)
	if err != nil {
		return err
	}
	if source == "-" {
		source = "stdin"
	}
	if !json.Valid(raw) {
		return fmt.Errorf("%s does not contain valid JSON", source)
	}

	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	m, err := store.GetRuntimeDeployment(ctx, selector)
	if err != nil {
		return err
	}

	// Hold the edit to the monitor's own struct. Storing unvalidated JSON would
	// fail every subsequent callback, with nothing able to overwrite it.
	canonical, err := monitor.ValidateState(ctx, m.ArtifactPath, filepath.Dir(m.ArtifactPath), raw, m.StateVersion)
	if err != nil {
		return fmt.Errorf("state from %s rejected by %s: %w", source, m.Name, err)
	}
	if _, err := store.ReplaceState(ctx, m.ID, m.StateRevision, canonical, m.StateVersion); err != nil {
		return err
	}

	fmt.Printf("state updated for %s (version %d, %d bytes)\n", m.Name, m.StateVersion, len(canonical))

	return nil
}

func (c *CLI) newStateClearCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clear NAME",
		Short: "Reset stored monitor state",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.stateClear(args[0])
		},
	}
}

func (c *CLI) stateClear(selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	m, err := store.GetRuntimeDeployment(ctx, selector)
	if err != nil {
		return err
	}

	// Empty input makes the monitor report its own defaults, so a cleared
	// monitor starts from the same state a fresh deploy would.
	defaults, err := monitor.ValidateState(ctx, m.ArtifactPath, filepath.Dir(m.ArtifactPath), nil, m.StateVersion)
	if err != nil {
		return err
	}
	if _, err := store.ReplaceState(ctx, m.ID, m.StateRevision, defaults, m.StateVersion); err != nil {
		return err
	}

	fmt.Printf("state cleared for %s (version %d, %d bytes)\n", m.Name, m.StateVersion, len(defaults))

	return nil
}

func readStateFile(path string) ([]byte, error) {
	if path == "-" {
		raw, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("read state from stdin: %w", err)
		}

		return raw, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}

	return raw, nil
}
