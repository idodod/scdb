package main

import (
	"context"
	"log"

	scdbclient "github.com/sjy-dv/scdb/scdb/pkg/scdbclient/v1"
)

func main() {
	conn, err := scdbclient.NewScdbConn("127.0.0.1:50051")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	err = conn.Ping(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	err = conn.Save(context.Background(), "greeting", []byte("helloworld"))
	if err != nil {
		log.Fatal(err)
	}
	val, err := conn.Get(context.Background(), "greeting")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(val))
	err = conn.Del(context.Background(), "greeting")
	if err != nil {
		log.Fatal(err)
	}
}
