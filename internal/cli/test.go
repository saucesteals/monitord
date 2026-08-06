package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/spf13/cobra"
)

// test builds a monitor and runs one tick locally without deploying it.
//
// Nothing is written to the schedule and no notification is sent; state changes
// are shown as a diff instead of being saved. This is the authoring loop.
func (c *CLI) newTestCmd() *cobra.Command {
	var useStored bool

	cmd := &cobra.Command{
		Use:   "test NAME",
		Short: "Build and run one monitor tick locally",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.test(args[0], useStored)
		},
	}
	cmd.Flags().BoolVar(&useStored, "stored-state", false, "start from the deployed monitor's stored state")

	return cmd
}

func (c *CLI) test(target string, useStored bool) error {
	paths, err := config.Init(c.root)
	if err != nil {
		return err
	}
	dir, name, err := monitor.ResolveDir(paths, target)
	if err != nil {
		return err
	}
	monitorConfig, err := monitor.LoadConfig(dir)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), monitorConfig.Timeout+30*time.Second)
	defer cancel()

	// Build to a temp dir so a test never disturbs deployed artifacts.
	buildDir, err := os.MkdirTemp("", "monitord-test-")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(buildDir) }()

	if err := config.Tidy(ctx, paths); err != nil {
		return err
	}

	binaryPath := filepath.Join(buildDir, "monitor")
	fmt.Printf("building %s\n", dir)
	build := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, ".")
	build.Dir = dir
	build.Env = append(os.Environ(), "GOWORK=off")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("build failed: %w", err)
	}

	described, err := monitor.Describe(ctx, binaryPath, dir)
	if err != nil {
		return err
	}
	def := described.Definition.WithDefaults()
	def.Name = name.String()
	def.Description = monitorConfig.Description
	def.Clients = monitorConfig.Clients
	def.Persistent = monitorConfig.Persistent
	before := described.State

	if useStored {
		store, _, err := c.store()
		if err != nil {
			return err
		}
		m, err := store.GetMonitor(ctx, name)
		_ = store.Close()
		if err != nil {
			return fmt.Errorf("--stored-state: %w", err)
		}
		if before, err = monitor.ValidateState(ctx, binaryPath, dir, m.State, m.StateVersion); err != nil {
			return err
		}
	}

	fmt.Printf("monitor  %s (clients %d, state v%d)\n", name, def.Clients, def.StateVersion)
	fmt.Printf("state in %s\n\n", compactJSON(before))

	after, status, err := runOneTick(ctx, binaryPath, dir, name, before, monitorConfig)
	if err != nil {
		return err
	}

	fmt.Printf("\nstate out %s\n", compactJSON(after))
	if string(before) != string(after) && len(after) > 0 {
		fmt.Println("state would be saved (differs from input)")
	}
	if status == monitord.StatusFailure {
		return fmt.Errorf("monitor tick failed")
	}

	return nil
}

// runOneTick drives a worker through a single tick and streams its output.
func runOneTick(
	ctx context.Context,
	binaryPath string,
	dir string,
	name model.MonitorName,
	state json.RawMessage,
	config monitor.Config,
) (json.RawMessage, monitord.ResultStatus, error) {
	cmd := exec.CommandContext(ctx, binaryPath, monitord.FlagWorker)
	cmd.Dir = dir
	cmd.Env = monitor.Env()
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, "", fmt.Errorf("open stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, "", fmt.Errorf("open stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, "", fmt.Errorf("start monitor: %w", err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	send := func(msg monitord.Inbound) error {
		payload, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		_, err = stdin.Write(append(payload, '\n'))

		return err
	}

	// A test runs direct, with no proxies, so the network assignment is empty.
	if err := send(monitord.Inbound{
		Type: monitord.InboundHello,
		Hello: &monitord.Hello{
			Monitor: monitord.MonitorName(name.String()),
			Dir:     dir,
			Network: monitord.Network{},
		},
	}); err != nil {
		return nil, "", err
	}

	started := time.Now().UTC()
	if err := send(monitord.Inbound{
		Type: monitord.InboundTick,
		Tick: &monitord.Tick{
			RunID:     "test",
			StartedAt: started,
			Deadline:  started.Add(config.Timeout),
			State:     state,
		},
	}); err != nil {
		return nil, "", err
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var msg monitord.Outbound
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			fmt.Println(line)

			continue
		}

		switch msg.Type {
		case monitord.OutboundReady:
			fmt.Printf("ready (%d client(s))\n\n", msg.Ready.Clients)
		case monitord.OutboundLog:
			fmt.Printf("[%s] %s\n", msg.Log.Level, msg.Log.Message)
		case monitord.OutboundEvent:
			fmt.Printf("[event/%s] %s: %s\n", msg.Event.Severity, msg.Event.Title, msg.Event.Summary)
			fmt.Printf("          id=%s\n", msg.Event.ID)
			fmt.Printf("          would deliver to %d destination(s)\n", len(config.Deliveries))
		case monitord.OutboundResult:
			fmt.Printf("\n[result] %s: %s\n", msg.Result.Status, msg.Result.Summary)
			if msg.Result.Details != "" {
				fmt.Println(indent(msg.Result.Details))
			}
			if msg.Result.Status == monitord.StatusFailure {
				fmt.Printf("would page %d destination(s) on the failure edge\n", len(config.Deliveries))
			}

			out := msg.Result.State
			if len(out) == 0 {
				out = state
			}

			return out, msg.Result.Status, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("read monitor output: %w", err)
	}

	return nil, "", fmt.Errorf("monitor exited without reporting a result")
}

func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}

	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return string(raw)
	}

	return out.String()
}
