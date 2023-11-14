package server

import (
	"net"
	"syscall"
)

// Sets additional keepalive parameters
// count: probe count
// interval: retry interval(seconds)
func setKeepaliveParameters(conn *net.TCPConn, count, interval int) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	err = rawConn.Control(
		func(fd uintptr) {
			// probe count
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, count)
			// retry interval after an unsuccessful probe
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, interval)
		})
	return err
}
