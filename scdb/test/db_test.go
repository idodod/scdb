package test

import (
	"testing"

	"github.com/sjy-dv/scdb/scdb/core"
)

func TestBasic(t *testing.T) {
	opt := core.DefaultOptions
	opt.DirPath = "/tmp/scdb"

	db, err := core.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Save([]byte("tester"), []byte("<>DATA!!>?"))
	if err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("tester"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(val))
}

func TestTx(t *testing.T) {
	opt := core.DefaultOptions
	opt.DirPath = "/tmp/scdb"

	db, err := core.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx := db.NewTransaction(core.DefaultTxOptions)
	tx.Save([]byte("tx11"), []byte("failed1"))
	tx.Save([]byte("tx21"), []byte("failed2"))
	tx.Save([]byte("tx31"), []byte("failed3"))
	t1, _ := tx.Get([]byte("tx11"))
	t2, _ := tx.Get([]byte("tx21"))
	t3, _ := tx.Get([]byte("tx31"))
	t.Log(string(t1), string(t2), string(t3))
	//tx.Rollback()
	tx.Commit()
	t1, _ = db.Get([]byte("tx11"))
	t2, _ = db.Get([]byte("tx21"))
	t3, _ = db.Get([]byte("tx31"))
	t.Log("relog", string(t1), string(t2), string(t3))
}
