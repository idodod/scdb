//go:build !linux
// +build !linux

package server

import (
	"net"
)

func setKeepaliveParameters(conn *net.TCPConn, count, interval int) error {
	return nil
}
