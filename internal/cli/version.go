package cli

import (
	"fmt"
	"io"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// version reports the build revision, which Go embeds from git at build time.
// Knowing exactly which commit a running daemon was built from is the first
// question when prod behaves differently from a working tree.
func (c *CLI) newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the build revision",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return c.version(cmd.OutOrStdout())
		},
	}
}

func (c *CLI) version(out io.Writer) error {
	fmt.Fprintf(out, "monitord %s\n", buildRevision())

	return nil
}

func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	revision, modified := "unknown", false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if len(setting.Value) >= 12 {
				revision = setting.Value[:12]
			} else {
				revision = setting.Value
			}
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if modified {
		revision += "-dirty"
	}

	return revision
}
