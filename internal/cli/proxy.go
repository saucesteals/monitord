package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/network"
	"github.com/saucesteals/monitord/internal/storage"
	"github.com/spf13/cobra"
)

func (c *CLI) newProxyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "Manage proxy pools",
		RunE: func(_ *cobra.Command, _ []string) error {
			return requireSubcommand("proxy", "import, list, show, rm")
		},
	}
	cmd.AddCommand(
		c.newProxyImportCmd(),
		c.newProxyListCmd(),
		c.newProxyShowCmd(),
		c.newProxyRemoveCmd(),
	)

	return cmd
}

func (c *CLI) proxyStore() (*storage.Store, error) {
	store, _, err := c.store()
	if err != nil {
		return nil, err
	}

	return store, nil
}

// proxyImport reads a proxy list into a stored pool. The file is only a
// transport: once imported, monitord owns the proxies, so the file can move or
// go away without affecting any monitor.
func (c *CLI) newProxyImportCmd() *cobra.Command {
	var strategy string

	cmd := &cobra.Command{
		Use:   "import NAME FILE",
		Short: "Import proxies into a pool",
		Args:  exactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			store, err := c.proxyStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return proxyImport(store, args[0], args[1], strategy)
		},
	}
	cmd.Flags().StringVar(&strategy, "strategy", "", "assignment: round_robin (default), random, sticky")

	return cmd
}

func proxyImport(store *storage.Store, rawName string, source string, strategyFlag string) error {
	name, err := model.ParsePoolName(rawName)
	if err != nil {
		return err
	}
	strategy, err := network.ParseStrategy(strategyFlag)
	if err != nil {
		return err
	}

	reader := os.Stdin
	if source != "-" {
		file, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("open proxy file: %w", err)
		}
		defer func() { _ = file.Close() }()
		reader = file
	}

	proxies, err := network.ParseProxies(reader)
	if err != nil {
		return err
	}

	ctx := context.Background()
	existing, existingErr := store.GetProxyPool(ctx, name)
	if err := store.UpsertProxyPool(ctx, storage.ProxyPool{
		Name:     name,
		Strategy: strategy,
		Proxies:  proxies,
	}); err != nil {
		return err
	}

	verb := "imported"
	if existingErr == nil {
		verb = fmt.Sprintf("replaced %d proxies in", len(existing.Proxies))
	}
	fmt.Printf("%s pool %s: %d proxies (%s)\n", verb, name, len(proxies), strategy)

	return nil
}

func (c *CLI) newProxyListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List proxy pools",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			store, err := c.proxyStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return proxyList(store)
		},
	}
}

func proxyList(store *storage.Store) error {
	pools, err := store.ListProxyPools(context.Background())
	if err != nil {
		return err
	}
	if len(pools) == 0 {
		fmt.Println("no proxy pools")

		return nil
	}

	fmt.Printf("%-20s %-14s %8s  %s\n", "NAME", "STRATEGY", "PROXIES", "UPDATED")
	for _, pool := range pools {
		fmt.Printf("%-20s %-14s %8d  %s\n",
			pool.Name, pool.Strategy, len(pool.Proxies), pool.UpdatedAt.Local().Format("Jan 02 15:04"))
	}

	return nil
}

// proxyShow prints a pool's shape without revealing credentials.
func (c *CLI) newProxyShowCmd() *cobra.Command {
	var reveal bool

	cmd := &cobra.Command{
		Use:   "show NAME",
		Short: "Show a proxy pool",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			store, err := c.proxyStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return proxyShow(store, args[0], reveal)
		},
	}
	cmd.Flags().BoolVar(&reveal, "reveal", false, "print full proxy URLs including credentials")

	return cmd
}

func proxyShow(store *storage.Store, rawName string, reveal bool) error {
	name, err := model.ParsePoolName(rawName)
	if err != nil {
		return err
	}

	pool, err := store.GetProxyPool(context.Background(), name)
	if err != nil {
		return err
	}

	fmt.Printf("name: %s\n", pool.Name)
	fmt.Printf("strategy: %s\n", pool.Strategy)
	fmt.Printf("proxies: %d\n", len(pool.Proxies))
	fmt.Printf("hosts: %d unique\n", uniqueHosts(pool.Proxies))
	fmt.Printf("updated: %s\n", pool.UpdatedAt.Local().Format("2006-01-02 15:04:05"))

	fmt.Println()
	for i, proxy := range pool.Proxies {
		if !reveal {
			proxy = network.Redact(proxy)
		}
		if i == 5 && !reveal {
			fmt.Printf("  ... and %d more\n", len(pool.Proxies)-5)

			break
		}
		fmt.Printf("  %s\n", proxy)
	}

	return nil
}

func (c *CLI) newProxyRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "rm NAME",
		Aliases: []string{"remove"},
		Short:   "Remove a proxy pool",
		Args:    exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			store, err := c.proxyStore()
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			return proxyRemove(store, args[0])
		},
	}
}

func proxyRemove(store *storage.Store, rawName string) error {
	name, err := model.ParsePoolName(rawName)
	if err != nil {
		return err
	}

	if err := store.DeleteProxyPool(context.Background(), name); err != nil {
		return err
	}
	fmt.Printf("removed proxy pool %s\n", name)

	return nil
}

func uniqueHosts(proxies []string) int {
	hosts := map[string]bool{}
	for _, proxy := range proxies {
		redacted := network.Redact(proxy)
		if host, _, ok := strings.Cut(strings.TrimPrefix(redacted, "http://"), ":"); ok {
			hosts[host] = true
		}
	}

	return len(hosts)
}
