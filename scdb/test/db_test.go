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
