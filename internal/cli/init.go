package cli

import (
	"fmt"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

func (c *CLI) newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize the monitord root",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return c.init()
		},
	}
}

func (c *CLI) init() error {
	paths, err := config.Init(c.root)
	if err != nil {
		return err
	}
	store, err := storage.Open(paths.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	fmt.Printf("initialized %s\n", paths.Root)

	return nil
}
