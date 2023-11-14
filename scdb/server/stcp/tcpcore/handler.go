package tcpcore

// EventHandler Conn events callback
type EventHandler interface {
	// OnOpened a new Conn has been opened
	OnOpened(c Conn)
	// OnClosed c has been closed
	OnClosed(c Conn)
	// OnReadMsg read one msg
	// data: body data
	// if err != nil, conn will be closed
	OnReadMsg(c Conn, data []byte) (err error)
	// OnWriteError an error occurred while writing data to c
	OnWriteError(c Conn, data []byte, err error)
}

func DefaultEventHandler() EventHandler {
	return &NetEventHandler{}
}

// NetEventHandler is a built-in implementation for EventHandler
type NetEventHandler struct {
}

func (h *NetEventHandler) OnOpened(c Conn) {
}

func (h *NetEventHandler) OnClosed(c Conn) {
}

func (h *NetEventHandler) OnReadMsg(c Conn, data []byte) (err error) {
	return nil
}

func (h *NetEventHandler) OnWriteError(c Conn, data []byte, err error) {
}
