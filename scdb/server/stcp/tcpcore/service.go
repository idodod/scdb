package tcpcore

type ServiceType uint32

const (
	SvcTypeTCPServer ServiceType = 1 << iota
	SvcTypeTCPClient
	SvcTypeTCPAsyncClient
	SvcTypeTCPPool
	SvcTypeTCPAsyncPool
)

func (t ServiceType) TCPServerType() bool {
	if t&SvcTypeTCPServer != 0 {
		return true
	}
	return false
}

func (t ServiceType) TCPClientType() bool {
	if t&SvcTypeTCPClient != 0 {
		return true
	}
	return false
}

func (t ServiceType) TCPAsyncClientType() bool {
	if t&SvcTypeTCPAsyncClient != 0 {
		return true
	}
	return false
}

func (t ServiceType) TCPPoolType() bool {
	if t&SvcTypeTCPPool != 0 {
		return true
	}
	return false
}

func (t ServiceType) TCPAsyncPoolType() bool {
	if t&SvcTypeTCPAsyncPool != 0 {
		return true
	}
	return false
}
