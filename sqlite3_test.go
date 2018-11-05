package sqlite3

import (
	"testing"
)

func TestNewDB(t *testing.T) {
	db, err := NewDB(":memory:")
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
}

func BenchmarkPrepare(b *testing.B) {
	db, err := NewDB(":memory:")
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
