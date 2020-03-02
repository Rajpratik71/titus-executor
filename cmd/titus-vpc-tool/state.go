package main

import (
	"context"

	"github.com/Netflix/titus-executor/vpc/tool/state"

	"github.com/spf13/cobra"
	pkgviper "github.com/spf13/viper"
)

func stateCmd(ctx context.Context, v *pkgviper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state",
		Short: "Introspect vpc state file",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "Introspect vpc state file",
		RunE: func(cmd *cobra.Command, args []string) error {
			return state.List(ctx, v.GetString(stateFileFlagName))
		},
	})

	addSharedFlags(cmd.PersistentFlags())

	return cmd
}
