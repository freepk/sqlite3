package sqlite3

import (
	"testing"
	"strconv"
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
	defer insStmt.Close()
	for i := 0; i < 40; i++ {
		err = insStmt.Exec(i, "test value" + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	selStmt, err := db.Prepare("SELECT `key`, `val` FROM `keyVal`")
	if err != nil {
		t.Fatal(err)
	}
	defer selStmt.Close()

	selStmt.next()
	/*
		for i := 0; i < 100; i++ {
			err = selStmt.Exec()
			if err != nil {
				t.Fatal(err)
			}
		}
	*/
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
