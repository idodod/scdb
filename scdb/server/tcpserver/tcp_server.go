package tcpserver

import (
	"errors"
	"os"
	"strings"

	"github.com/sjy-dv/scdb/scdb/launch"
	"github.com/sjy-dv/scdb/scdb/pkg/log"
	"github.com/sjy-dv/scdb/scdb/server/stcp"
	"github.com/sjy-dv/scdb/scdb/server/stcp/tcpcore"
)

type ServerHandler struct {
	launcher *launch.ScLauncher
}

func NewServerHandler(l *launch.ScLauncher) *ServerHandler {
	return &ServerHandler{
		launcher: l,
	}
}

func ServeTcp(launch *launch.ScLauncher) {
	svc := stcp.NewService(
		tcpcore.WithServiceType(tcpcore.SvcTypeTCPServer),
		tcpcore.WithAddr(launch.RunningTcpPort),
		tcpcore.WithHeartbeat([]byte{0}, 0),
		tcpcore.WithEventHandler(NewServerHandler(launch)),
	)
	s := svc.Server()
	if err := s.Init(); err != nil {
		log.Error("Internal TCP Server Crash Exit!!")
		os.Exit(0)
	} else {
		log.Info("Internal TCP Server Starting...")
	}
	s.Serve()
}

func (h *ServerHandler) OnOpened(c tcpcore.Conn) {}
func (h *ServerHandler) OnClosed(c tcpcore.Conn) {}
func (h *ServerHandler) OnReadMsg(c tcpcore.Conn, data []byte) error {
	br, err := BytesDecode[BytesRequest](data)
	if err != nil {
		msg, _ := BytesEncode[BytesResponse](&BytesResponse{
			Error:       err,
			ErrorStatus: true,
		})
		c.Write(msg)
	}
	reply, _ := h.SwitchProcess(br)
	c.Write(reply)
	return nil
}
func (h *ServerHandler) OnWriteError(c tcpcore.Conn, data []byte, err error) {}

func (h *ServerHandler) SwitchProcess(dataType *BytesRequest) ([]byte, error) {
	switch strings.ToUpper(dataType.Method) {
	case "PING":
		reply, err := BytesEncode[BytesResponse](&BytesResponse{ErrorStatus: false})
		if err != nil {
			return nil, err
		}
		return reply, nil
	case "SET":
		err := h.launcher.SCDB.Save(dataType.Key, dataType.Value)
		if err != nil {
			reply, err := BytesEncode[BytesResponse](&BytesResponse{Error: err, ErrorStatus: true})
			if err != nil {
				return nil, err
			}
			return reply, nil
		}
		reply, err := BytesEncode[BytesResponse](&BytesResponse{ErrorStatus: false})
		if err != nil {
			return nil, err
		}
		return reply, nil
	case "GET":
		val, err := h.launcher.SCDB.Get(dataType.Key)
		if err != nil {
			reply, err := BytesEncode[BytesResponse](&BytesResponse{Error: err, ErrorStatus: true})
			if err != nil {
				return nil, err
			}
			return reply, nil
		}
		reply, err := BytesEncode[BytesResponse](&BytesResponse{ErrorStatus: false, Value: val})
		if err != nil {
			return nil, err
		}
		return reply, nil
	case "DEL":
		err := h.launcher.SCDB.Delete(dataType.Key)
		if err != nil {
			reply, err := BytesEncode[BytesResponse](&BytesResponse{ErrorStatus: true, Error: err})
			if err != nil {
				return nil, err
			}
			return reply, nil
		}
		reply, err := BytesEncode[BytesResponse](&BytesResponse{ErrorStatus: false})
		if err != nil {
			return nil, err
		}
		return reply, nil
	default:
		return nil, errors.New("unknown")
	}
}
