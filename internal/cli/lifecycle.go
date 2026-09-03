package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func (c *CLI) newArchiveCmd() *cobra.Command {
	return &cobra.Command{Use: "archive NAME_OR_ID", Short: "Archive an inactive deployment", Args: exactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return c.archive(cmd.Context(), cmd.OutOrStdout(), args[0])
	}}
}

func (c *CLI) archive(ctx context.Context, out io.Writer, selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	deployment, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}
	if deployment.Status == "active" {
		return errors.New("pause the deployment before archiving it")
	}
	if err := store.ArchiveDeployment(ctx, deployment.ID); err != nil {
		return err
	}
	fmt.Fprintf(out, "archived %s (%s)\n", deployment.Name, deployment.ID)
	return nil
}

func (c *CLI) newPurgeCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{Use: "purge NAME_OR_ID", Short: "Permanently delete an archived deployment", Args: exactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return c.purge(cmd.Context(), cmd.OutOrStdout(), args[0], force)
	}}
	cmd.Flags().BoolVar(&force, "force", false, "discard queued deliveries")
	return cmd
}

func (c *CLI) purge(ctx context.Context, out io.Writer, selector string, force bool) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	deployment, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}
	if deployment.Status != "archived" {
		return errors.New("archive the deployment before purging it")
	}
	if err = store.PurgeDeploymentSafe(ctx, deployment.ID, force); err != nil {
		if strings.Contains(err.Error(), "queued deliveries") {
			return errors.New("queued deliveries remain; retry later or use --force")
		}
		return err
	}
	fmt.Fprintf(out, "purged %s (%s)\n", deployment.Name, deployment.ID)
	return nil
}

func (c *CLI) newPauseCmd() *cobra.Command {
	return &cobra.Command{Use: "pause NAME_OR_ID", Short: "Pause a deployment", Args: exactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return c.pause(cmd.Context(), cmd.OutOrStdout(), args[0])
	}}
}

func (c *CLI) pause(ctx context.Context, out io.Writer, selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	deployment, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}
	if err := store.PauseDeployment(ctx, deployment.ID); err != nil {
		return err
	}
	fmt.Fprintf(out, "paused %s (%s)\n", deployment.Name, deployment.ID)
	return nil
}

func (c *CLI) newResumeCmd() *cobra.Command {
	var ttl time.Duration
	var persistent bool
	cmd := &cobra.Command{Use: "resume NAME_OR_ID", Short: "Resume an inactive deployment", Args: exactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if persistent == (ttl > 0) {
			return errors.New("set exactly one of --ttl or --persistent")
		}
		store, _, err := c.store()
		if err != nil {
			return err
		}
		defer func() { _ = store.Close() }()
		deployment, err := store.GetDeployment(cmd.Context(), args[0])
		if err != nil {
			return err
		}
		var expires *time.Time
		if ttl > 0 {
			value := time.Now().UTC().Add(ttl)
			expires = &value
		}
		if err = store.ResumeDeployment(cmd.Context(), deployment.ID, expires); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "resumed %s (%s)\n", deployment.Name, deployment.ID)
		return nil
	}}
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "fresh deployment lifetime")
	cmd.Flags().BoolVar(&persistent, "persistent", false, "resume without an expiration")
	return cmd
}
