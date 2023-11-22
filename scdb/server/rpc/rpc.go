package rpc

import (
	"fmt"
	"net"
	"os"
	"runtime/debug"

	"github.com/sjy-dv/scdb/scdb/launch"
	"github.com/sjy-dv/scdb/scdb/pkg/log"
	"github.com/sjy-dv/scdb/scdb/pkg/protobuf/scdbpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type RpcServerBase struct {
	*launch.ScLauncher
	InternalServer *InternalRpcServer
}

type InternalRpcServer struct {
	scdbpb.UnimplementedScdbServer
	RpcServerBase
}

func ServeRpc(launcher *launch.ScLauncher) {
	defer func() {
		if r := recover(); r != nil {
			log.Error(fmt.Sprintf("RPC Server Error %v:%v", r, debug.Stack()))
		}
	}()
	r := RpcServerBase{ScLauncher: launcher}
	r.InternalServer = &InternalRpcServer{RpcServerBase: r}
	r.Serve()
}

func (r RpcServerBase) Serve() {
	lis, err := net.Listen("tcp", r.RunningRpcPort)
	if err != nil {
		log.Warn(fmt.Sprintf("FAILED START LISTENING : %v", err))
	}
	grpcServer := grpc.NewServer()
	scdbpb.RegisterScdbServer(grpcServer, r.InternalServer)
	reflection.Register(grpcServer)
	log.Info("SCDB Register Internal gRPC SERVER")
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("Internal gRPC Server Crash Exit!!")
			os.Exit(0)
		} else {
			log.Info("Internal gRPC Server Starting...")
		}
	}()
}
