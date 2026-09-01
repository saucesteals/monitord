package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

func (c *CLI) newCheckpointsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checkpoints",
		Short: "Manage durable source checkpoints",
		RunE: func(_ *cobra.Command, _ []string) error {
			return requireSubcommand("checkpoints", "clear")
		},
	}
	cmd.AddCommand(c.newCheckpointsClearCmd())

	return cmd
}

func (c *CLI) newCheckpointsClearCmd() *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "clear NAME_OR_ID --all",
		Short: "Clear every checkpoint for an inactive deployment",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !all {
				return errors.New("pass --all to clear every checkpoint")
			}
			return c.checkpointsClear(cmd.Context(), cmd.OutOrStdout(), args[0])
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "confirm clearing every checkpoint")

	return cmd
}

func (c *CLI) checkpointsClear(ctx context.Context, out io.Writer, selector string) error {
	store, _, err := c.store()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	deployment, err := store.GetDeployment(ctx, selector)
	if err != nil {
		return err
	}
	if deployment.Status != "inactive" {
		return errors.New("pause the deployment before clearing checkpoints")
	}

	count, err := store.ClearCheckpoints(ctx, deployment.ID)
	if errors.Is(err, storage.ErrInvalidStatus) {
		return errors.New("pause the deployment before clearing checkpoints")
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "cleared %d checkpoint(s) for %s (%s)\n", count, deployment.Name, deployment.ID)
	return nil
}
