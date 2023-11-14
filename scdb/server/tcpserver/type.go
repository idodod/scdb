package tcpserver

import "encoding/json"

type BytesRequest struct {
	Method string
	Key    []byte
	Value  []byte
}

type BytesResponse struct {
	Value       []byte
	Error       error
	ErrorStatus bool
}

type BytesGroup interface {
	BytesRequest | BytesResponse
}

func BytesEncode[bg BytesGroup](b *bg) ([]byte, error) {
	return json.Marshal(b)
}

func BytesDecode[bg BytesGroup](data []byte) (*bg, error) {
	var b bg
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, err
	}
	return &b, nil
}
