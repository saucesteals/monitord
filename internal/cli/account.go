package cli

import (
	"fmt"
	"strings"

	"github.com/saucesteals/monitord/internal/routes"
	"github.com/spf13/cobra"
)

func (c *CLI) newAccountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "account",
		Short: "Manage delivery credentials",
		RunE: func(_ *cobra.Command, _ []string) error {
			return requireSubcommand("account", "set, list, remove")
		},
	}
	cmd.AddCommand(c.newAccountSetCmd(), c.newAccountListCmd(), c.newAccountRemoveCmd())

	return cmd
}

func (c *CLI) newAccountListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List Keychain delivery accounts",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			accounts, err := routes.ListAccounts(cmd.Context())
			if err != nil {
				return err
			}
			for _, account := range accounts {
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", account.Kind, account.Name)
			}

			return nil
		},
	}
}

func (c *CLI) newAccountRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove KIND NAME",
		Short: "Remove a Keychain delivery account",
		Args:  exactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := routes.RemoveAccount(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "removed %s account %s from Keychain\n", args[0], args[1])

			return nil
		},
	}
}

func (c *CLI) newAccountSetCmd() *cobra.Command {
	var token string

	cmd := &cobra.Command{
		Use:   "set KIND NAME",
		Short: "Store a delivery account token in Keychain",
		Args:  exactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if token == "" {
				return fmt.Errorf("--token is required")
			}
			if err := routes.StoreAccountToken(cmd.Context(), args[0], args[1], strings.TrimSpace(token)); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "stored %s account %s in Keychain\n", args[0], args[1])

			return nil
		},
	}
	cmd.Flags().StringVar(&token, "token", "", "delivery account token")

	return cmd
}
