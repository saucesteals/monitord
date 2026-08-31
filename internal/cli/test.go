package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
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
	secretresolver "github.com/saucesteals/monitord/internal/secrets"
	"github.com/spf13/cobra"
)

// test builds a monitor and runs one callback locally without deploying it.
//
// Nothing is written to the schedule and no notification is sent; state changes
// are shown as a diff instead of being saved. This is the authoring loop.
func (c *CLI) newTestCmd() *cobra.Command {
	var useStored bool
	var duration time.Duration

	cmd := &cobra.Command{
		Use:   "test NAME",
		Short: "Build and run one monitor callback locally",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.test(args[0], useStored, duration)
		},
	}
	cmd.Flags().BoolVar(&useStored, "stored-state", false, "start from the deployed monitor's stored state")
	cmd.Flags().DurationVar(&duration, "duration", 30*time.Second, "maximum local run duration")

	return cmd
}

func (c *CLI) test(target string, useStored bool, duration time.Duration) error {
	if duration <= 0 {
		return fmt.Errorf("--duration must be positive")
	}
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

	ctx, cancel := context.WithTimeout(context.Background(), duration)
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
	before := described.State
	refs := make([]secretresolver.Ref, 0, len(described.Plan.SecretRefs()))
	for _, ref := range described.Plan.SecretRefs() {
		refs = append(refs, secretresolver.Ref{Group: ref.Group, Key: ref.Key, Required: ref.Required})
	}
	resolved, err := secretresolver.Resolve(refs, secretresolver.Sources{Root: paths.Root, MonitorDir: dir})
	if err != nil {
		return fmt.Errorf("resolve test secrets: %w", err)
	}
	workerSecrets := map[string]map[string]string{}
	for _, value := range resolved {
		if workerSecrets[value.Ref.Group] == nil {
			workerSecrets[value.Ref.Group] = map[string]string{}
		}
		workerSecrets[value.Ref.Group][value.Ref.Key] = value.Value
	}

	if useStored {
		store, _, err := c.store()
		if err != nil {
			return err
		}
		m, err := store.GetDeployment(ctx, name.String())
		_ = store.Close()
		if err != nil {
			return fmt.Errorf("--stored-state: %w", err)
		}
		if before, err = monitor.ValidateState(ctx, binaryPath, dir, m.State, m.StateVersion); err != nil {
			return err
		}
	}

	fmt.Printf("monitor  %s (%s, state v%d)\n", name, described.Info.Name, described.StateVersion)
	fmt.Printf("state in %s\n\n", compactJSON(before))

	after, status, err := runLocal(ctx, binaryPath, dir, name, before, described.Plan, workerSecrets, monitorConfig)
	if err != nil {
		return err
	}

	fmt.Printf("\nstate out %s\n", compactJSON(after))
	if string(before) != string(after) && len(after) > 0 {
		fmt.Println("state would be saved (differs from input)")
	}
	if status == "failure" {
		return fmt.Errorf("monitor callback failed")
	}

	return nil
}

// runLocal drives a worker through one local callback and streams its output.
func runLocal(
	ctx context.Context,
	binaryPath string,
	dir string,
	name model.MonitorName,
	state json.RawMessage,
	plan monitord.PlanDescription,
	secrets map[string]map[string]string,
	config monitor.Config,
) (json.RawMessage, string, error) {
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

	send := func(msg monitord.DaemonFrame) error {
		payload, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		_, err = stdin.Write(append(payload, '\n'))

		return err
	}

	if err := send(monitord.DaemonFrame{
		Type:  "hello",
		Hello: &monitord.Hello{Version: monitord.ProtocolVersion{Major: monitord.ProtocolMajor}, DeploymentID: "local-test", DeploymentName: name.String(), Generation: 1, WorkerToken: "local-test-token", ArtifactHash: "local", ConfigHash: "local", State: state, Secrets: secrets},
	}); err != nil {
		return nil, "", err
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	current, revision, started, status := append(json.RawMessage(nil), state...), int64(0), false, "success"
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var msg monitord.WorkerFrame
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			fmt.Println(line)

			continue
		}

		switch msg.Type {
		case "monitor":
			if err := send(monitord.DaemonFrame{Type: "start", Start: &monitord.Start{Plan: plan}}); err != nil {
				return nil, "", err
			}
			started = true
		case "ready":
			fmt.Println("ready")
		case "transaction":
			if monitord.HashTransactionFrame(*msg.Transaction) != mustHash(msg.Transaction.PayloadHash) {
				return nil, "", fmt.Errorf("worker transaction hash mismatch")
			}
			for _, event := range msg.Transaction.Events {
				fmt.Printf("[event/%s] %s: %s\n          id=%s\n          would deliver to %d destination(s)\n", event.Severity, event.Title, event.Summary, event.ID, len(config.Deliveries))
			}
			current = append(current[:0], msg.Transaction.NextState...)
			revision++
			if err := send(monitord.DaemonFrame{Type: "ack", Ack: &monitord.TransactionAck{DeploymentID: msg.Transaction.DeploymentID, Generation: msg.Transaction.Generation, Sequence: msg.Transaction.Sequence, PayloadHash: msg.Transaction.PayloadHash, ResultRevision: revision, Status: "accepted"}}); err != nil {
				return nil, "", err
			}
			if plan.Kind == "continuous" && started {
				_ = send(monitord.DaemonFrame{Type: "stop", Stop: &monitord.Stop{Reason: "local test complete"}})
				started = false
			}
		case "run":
			if msg.Run.Phase == "finished" {
				if msg.Run.Error != "" {
					status = "failure"
					fmt.Printf("[run] failed: %s\n", msg.Run.Error)
				} else {
					fmt.Println("[run] success")
				}
				_ = send(monitord.DaemonFrame{Type: "stop", Stop: &monitord.Stop{Reason: "local test complete"}})
			}
		case "health":
			fmt.Printf("[health/%s] %s %s\n", msg.Health.Child, msg.Health.Status, msg.Health.Message)
		case "stopped":
			_ = stdin.Close()
			_ = cmd.Wait()
			return current, status, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("read monitor output: %w", err)
	}

	return nil, "", fmt.Errorf("monitor exited without a stopped frame")
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

func mustHash(value string) [32]byte {
	var out [32]byte
	decoded, _ := hex.DecodeString(value)
	copy(out[:], decoded)
	return out
}
