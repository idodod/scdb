package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sjy-dv/scdb/scdb/pkg/log"
	"github.com/sjy-dv/scdb/scdb/server/stcp"
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
	log.Info(c.GetTag(), "AsyncClient read msg:", string(data))
	return nil
}

func (h *AsyncHandler) OnWriteError(c tcpcore.Conn, data []byte, err error) {
	log.Warn(c.GetTag(), "AsyncClient write error:", err)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for i := 0; i < 1; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				go Client(i)
			}
		}
	}()
	time.Sleep(10 * time.Millisecond)

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	select {
	case sig := <-c:
		fmt.Println("Exit with signal:", sig)
	}
}

func Client(id int) {
	svc := stcp.NewService(
		tcpcore.WithServiceType(tcpcore.SvcTypeTCPClient),
		tcpcore.WithAddr("127.0.0.1:6727"),
	)
	c := svc.Client()
	if err := c.Init(); err != nil {
		log.Error("client init error:", err)
		return
	}
	defer c.Close()

	data, _ := tcpserver.BytesEncode[tcpserver.BytesRequest](&tcpserver.BytesRequest{
		Method: "get",
		Key:    []byte("tester"),
	})
	for i := 0; i < 10; i++ {
		resp, err := c.WriteRead(data)
		if err != nil {
			log.Error(err)
			return
		}
		log.Info("recv resp:", string(resp), i)
		time.Sleep(1 * time.Second)
	}
	log.Info("Client Done:", id)
}

func AsyncClient(id int) {
	svc := stcp.NewService(
		tcpcore.WithServiceType(tcpcore.SvcTypeTCPAsyncClient),
		tcpcore.WithAddr("127.0.0.1:6727"),
		tcpcore.WithEventHandler(NewAsyncHandler()),
		tcpcore.WithHeartbeat([]byte{0}, 5*time.Second),
	)
	c := svc.Client()
	if err := c.Init(); err != nil {
		log.Error("async client init error:", err)
		return
	}
	c.SetTag(strconv.Itoa(id))
	defer c.Close()

	data := []byte("Hello world " + strconv.Itoa(id))
	for i := 0; i < 3; i++ {
		if err := c.Write(data); err != nil {
			log.Error(id, "write err:", err)
			return
		}
		time.Sleep(2 * time.Second)
	}
	log.Info("AsyncClient Done:", id)
}
