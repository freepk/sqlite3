package sqlite3

import (
	"encoding/binary"
	"testing"
)

func TestContainsFunc(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.RegisterContainsFunc()
	if err != nil {
		t.Fatal(err)
	}

	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` BLOB NOT NULL) WITHOUT ROWID")
	if err != nil {
		t.Fatal(err)
	}
	defer tabStmt.Close()
	err = tabStmt.Exec()

	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		t.Fatal(err)
	}
	defer insStmt.Close()
	for i := 0; i < 3; i++ {
		id := i + 1
		m := i + 3
		blob := make([]byte, m*4)
		for j := 0; j < m; j++ {
			n := id*1000 + 100*j
			t.Log("n=", n)
			binary.LittleEndian.PutUint32(blob[j*4:j*4+4], uint32(n))
		}
		t.Log("blob=", blob)
		err = insStmt.Exec(id, string(blob))
		if err != nil {
			t.Fatal(err)
		}
	}

	stmt, err := db.Prepare("SELECT `key` FROM `keyVal` WHERE contains(`val`, 2300)")
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for stmt.Next() {
		t.Log(stmt.RowBytes())
	}
}
