package test

import (
	"log"

	"github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
	"github.com/sjy-dv/scdb/scdb/server/tcpserver"
)

type AsyncHandler struct {
	*tcpcore.NetEventHandler
}

func NewAsyncHandler() *AsyncHandler {
	return &AsyncHandler{}
}

func (h *AsyncHandler) OnReadMsg(c tcpcore.Conn, data []byte) error {
	val, _ := tcpserver.BytesDecode[tcpserver.BytesResponse](data)
	log.Println(string(val.Value), val.Error, val.ErrorStatus)
	return nil
}
