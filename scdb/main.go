package main

import (
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/sjy-dv/scdb/scdb/launch"
	"github.com/sjy-dv/scdb/scdb/server/rpc"
	"github.com/sjy-dv/scdb/scdb/server/tcpserver"
)

func init() {
	godotenv.Load()
	os.Setenv("TZ", "UTC")
	time.Local = time.UTC
}

func main() {
	BootSystem()
}

func BootSystem() {
	launcher := launch.LoadEnv()
	launcher.LaunchSolidCoreSystem()
	go rpc.ServeRpc(launcher)
	go tcpserver.ServeTcp(launcher)
	select {}
}
