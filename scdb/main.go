package main

import (
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/sjy-dv/scdb/scdb/launch"
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
}
