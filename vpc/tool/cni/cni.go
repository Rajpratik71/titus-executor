package cni

import (
	"context"
	"github.com/Netflix/titus-executor/fslocker"
	"github.com/Netflix/titus-executor/vpc/tool/identity"
	"github.com/containernetworking/cni/pkg/skel"
	"google.golang.org/grpc"
)

type CNICommand struct {
	Add func(skel *skel.CmdArgs) error
	Check func(skel *skel.CmdArgs) error
	Del func (skel *skel.CmdArgs) error
}

type GetSharedValues func (ctx context.Context) (*fslocker.FSLocker, *grpc.ClientConn, error)

func MakeCommand(ctx context.Context, instanceIdentityProvider identity.InstanceIdentityProvider, gsv GetSharedValues) *CNICommand {
	return nil
}