package sqlite3

import (
	"encoding/binary"
	"strconv"
	"testing"
)

func TestCommon(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		t.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		err = insStmt.Exec(i, ("test value" + strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	insStmt.Close()
	selStmt, err := db.Prepare("SELECT `key`, `val` FROM `keyVal`")
	if err != nil {
		t.Fatal(err)
	}
	defer selStmt.Close()

	selStmt.Exec()
	i := 0
	for selStmt.Next() {
		i++
		t.Log(i, selStmt.RowBytes())
	}
}

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

func TestBackupRestore(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		t.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		t.Fatal(err)
	}
	defer insStmt.Close()
	for i := 0; i < 100; i++ {
		err = insStmt.Exec(i, "test value")
		if err != nil {
			t.Fatal(err)
		}
	}
	err = db.Backup("backup.db")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Restore("backup.db")
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPrepare(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := db.Prepare("SELECT 1 x UNION ALL SELECT 2 UNION ALL SELECT 3")
		if err != nil {
			b.Fatal(err)
		}
		stmt.Close()
	}
}

func BenchmarkBind(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.Prepare("SELECT ?1 x UNION ALL SELECT ?2")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = stmt.bind(i, "test value")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecSelect(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.Prepare("SELECT ?1 x UNION ALL SELECT ?2")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = stmt.Exec(i, "test value")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecInsert(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		b.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		b.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		b.Fatal(err)
	}
	defer insStmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = insStmt.Exec(i, "test value")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRestore(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		b.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		b.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		b.Fatal(err)
	}
	defer insStmt.Close()
	for i := 0; i < 100; i++ {
		err = insStmt.Exec(i, "test value")
		if err != nil {
			b.Fatal(err)
		}
	}
	err = db.Backup("backup.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = db.Restore("backup.db")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNext1000Int(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		b.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		b.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		err = insStmt.Exec(i, ("test value " + strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
	}
	insStmt.Close()
	selStmt, err := db.Prepare("SELECT `key` FROM `keyVal`")
	if err != nil {
		b.Fatal(err)
	}
	defer selStmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selStmt.Exec()
		for selStmt.Next() {
		}
	}
}

func BenchmarkNext1000IntStr(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		b.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		b.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		err = insStmt.Exec(i, ("test value " + strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
	}
	insStmt.Close()
	selStmt, err := db.Prepare("SELECT `key`, `val` FROM `keyVal`")
	if err != nil {
		b.Fatal(err)
	}
	defer selStmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selStmt.Exec()
		for selStmt.Next() {
		}
	}
}

func BenchmarkNext10000Int(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		b.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		b.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		err = insStmt.Exec(i, ("test value " + strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
	}
	insStmt.Close()
	selStmt, err := db.Prepare("SELECT `key` FROM `keyVal`")
	if err != nil {
		b.Fatal(err)
	}
	defer selStmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selStmt.Exec()
		for selStmt.Next() {
		}
	}
}

func BenchmarkNext10000IntStr(b *testing.B) {
	db, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tabStmt, err := db.Prepare("CREATE TABLE `keyVal` (`key` INT NOT NULL CONSTRAINT `PK_keyVal` PRIMARY KEY, `val` TEXT NOT NULL) WITHOUT ROWID")
	if err != nil {
		b.Fatal(err)
	}
	err = tabStmt.Exec()
	if err != nil {
		b.Fatal(err)
	}
	tabStmt.Close()
	insStmt, err := db.Prepare("INSERT INTO `keyVal` (`key`, `val`) VALUES (?1, ?2)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		err = insStmt.Exec(i, ("test value " + strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
	}
	insStmt.Close()
	selStmt, err := db.Prepare("SELECT `key`, `val` FROM `keyVal`")
	if err != nil {
		b.Fatal(err)
	}
	defer selStmt.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selStmt.Exec()
		for selStmt.Next() {
		}
	}
}
