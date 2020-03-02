package main

import (
	"context"
	"github.com/Netflix/titus-executor/fslocker"
	"github.com/Netflix/titus-executor/vpc/tool/cni"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	pkgviper "github.com/spf13/viper"
	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/version"
	"google.golang.org/grpc"
)

func cniCommand(ctx context.Context, v *pkgviper.Viper, iipGetter instanceIdentityProviderGetter) *cobra.Command {
	versionInfo := version.PluginSupports("0.3.0", "0.3.1")
	cmd := &cobra.Command{
		Use:   "cni",
		Short: "Run as CNI plugin",
		RunE: func(cmd *cobra.Command, args []string) error {
			cniCommand := cni.MakeCommand(ctx, iipGetter(), func(ctx2 context.Context) (*fslocker.FSLocker, *grpc.ClientConn, error) {
				return  getSharedValues(ctx2, v)
			})
			err := skel.PluginMainWithError(cniCommand.Add, cniCommand.Check, cniCommand.Del, versionInfo, "Titus CNI Plugin")
			if err != nil {
				err2 := err.Print()
				if err2 != nil {
					err2 = errors.Wrap(err, "Cannot write error JSON")
					return err2
				}
			}
			return err
		},
	}

	addSharedFlags(cmd.Flags())
	return cmd
}

