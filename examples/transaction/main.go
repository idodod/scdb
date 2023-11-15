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
	tx := conn.Begin()
	// transaction batch-create
	err = tx.Save(context.Background(), "tx1", []byte("hello tx1"))
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	err = tx.Save(context.Background(), "tx2", []byte("hello tx2"))
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	err = tx.Save(context.Background(), "tx3", []byte("hello tx3"))
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	err = tx.Save(context.Background(), "tx4", []byte("hello tx4"))
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	// check save value
	val1, err := tx.Get(context.Background(), "tx1")
	log.Println("v1", string(val1), err)
	val2, err := tx.Get(context.Background(), "tx2")
	log.Println("v2", string(val2), err)
	val3, err := tx.Get(context.Background(), "tx3")
	log.Println("v3", string(val3), err)
	val4, err := tx.Get(context.Background(), "tx4")
	log.Println("v4", string(val4), err)
	tx.Commit()
	//if tx.rollback. You cannot retrieve the value again.
	val1, err = conn.Get(context.Background(), "tx1")
	log.Println(string(val1), err)
	val2, err = conn.Get(context.Background(), "tx2")
	log.Println(string(val2), err)
	val3, err = conn.Get(context.Background(), "tx3")
	log.Println(string(val3), err)
	val4, err = conn.Get(context.Background(), "tx4")
	log.Println(string(val4), err)

}
