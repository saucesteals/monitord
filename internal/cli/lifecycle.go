package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newRemoveCmd() *cobra.Command {
	var purge bool
	var force bool

	cmd := &cobra.Command{
		Use:     "rm NAME",
		Aliases: []string{"remove"},
		Short:   "Delete a monitor",
		Args:    exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.rm(args[0], purge, force)
		},
	}
	cmd.Flags().BoolVar(&purge, "purge", false, "permanently delete archived deployment data")
	cmd.Flags().BoolVar(&force, "force", false, "allow purge when queued deliveries remain")

	return cmd
}

func (c *CLI) rm(selector string, purge, force bool) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	d, err := store.GetDeployment(context.Background(), selector)
	if err != nil {
		return err
	}
	if !purge {
		if d.Status != "archived" {
			err = store.ArchiveDeployment(context.Background(), d.ID)
		}
		if err == nil {
			fmt.Printf("archived %s (%s)\n", d.Name, d.ID)
		}
		return err
	}
	if d.Status != "archived" {
		return errors.New("archive deployment before purging")
	}
	if err = store.PurgeDeploymentSafe(context.Background(), d.ID, force); err != nil {
		if strings.Contains(err.Error(), "queued deliveries") {
			return errors.New("queued deliveries remain; retry later or use --force")
		}
		return err
	}
	fmt.Printf("purged %s (%s)\n", d.Name, d.ID)
	return nil
}

func (c *CLI) newExpireCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "expire NAME",
		Short: "Stop scheduling a monitor",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.expire(args[0])
		},
	}
}

func (c *CLI) newResumeCmd() *cobra.Command {
	var ttl time.Duration
	cmd := &cobra.Command{Use: "resume NAME_OR_ID", Short: "Resume an expired deployment", Args: exactArgs(1), RunE: func(_ *cobra.Command, args []string) error {
		if ttl <= 0 {
			return errors.New("--ttl must be positive")
		}
		store, _, err := c.store()
		if err != nil {
			return err
		}
		defer store.Close()
		d, err := store.GetDeployment(context.Background(), args[0])
		if err != nil {
			return err
		}
		expires := time.Now().UTC().Add(ttl)
		if err = store.ResumeDeployment(context.Background(), d.ID, &expires); err != nil {
			return err
		}
		fmt.Printf("resumed %s (%s) until %s\n", d.Name, d.ID, expires.Format(time.RFC3339))
		return nil
	}}
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "fresh deployment lifetime")
	_ = cmd.MarkFlagRequired("ttl")
	return cmd
}

func (c *CLI) expire(rawName string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	d, err := store.GetDeployment(context.Background(), rawName)
	if err != nil {
		return err
	}
	if err := store.ExpireDeployment(context.Background(), d.ID); err != nil {
		return err
	}
	fmt.Printf("expired %s (%s)\n", d.Name, d.ID)

	return nil
}
