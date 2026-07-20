package cli

import (
	"fmt"

	"github.com/saucesteals/monitord"
	"github.com/spf13/cobra"
)

func (c *CLI) newSkillCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "skill",
		Short: "Print the SKILL.md for use with AI agents",
		Long: `Print the embedded SKILL.md to stdout.

Pipe it into an AI agent skill directory to enable autonomous monitoring:

  mkdir -p /path/to/agent/skills/monitord
  monitord skill > /path/to/agent/skills/monitord/SKILL.md`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprint(cmd.OutOrStdout(), monitord.SkillMD)
			return nil
		},
	}
}
