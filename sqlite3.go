package sqlite3

/*
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

#define URI_MAX_SIZE 256
#define TRUE (1==1)
#define FALSE (!TRUE)

int _sqlite3_open(sqlite3 **ppDb, _GoString_ URI) {
	size_t size = _GoStringLen(URI);
	if (size >= URI_MAX_SIZE) {
		return SQLITE_ERROR;
	}
	char cURI[URI_MAX_SIZE];
	memcpy(cURI, _GoStringPtr(URI), size);
	cURI[size] = 0;
	return sqlite3_open(cURI, ppDb);
}

int _sqlite3_copy(sqlite3 *pDb, _GoString_ URI, int isSave) {
	sqlite3 *pTemp;
	int rc = _sqlite3_open(&pTemp, URI);
	if (rc == SQLITE_OK) {
		sqlite3 *pTo;
		sqlite3 *pFrom;
		if (isSave == 1) {
			pTo = pTemp;
			pFrom = pDb;
		} else {
			pTo = pDb;
			pFrom = pTemp;
		}
		sqlite3_backup *pBackup = sqlite3_backup_init(pTo, "main", pFrom, "main");
		if (pBackup) {
			(void)sqlite3_backup_step(pBackup, -1);
			(void)sqlite3_backup_finish(pBackup);
		}
		rc = sqlite3_errcode(pTo);
	}
	sqlite3_close(pTemp);
	return rc;
}

int _sqlite3_prepare(sqlite3 *pDb, sqlite3_stmt **ppStmt, _GoString_ SQL) {
	return sqlite3_prepare_v2(pDb, _GoStringPtr(SQL), _GoStringLen(SQL), ppStmt, NULL);
}

int _sqlite3_bind_text_static(sqlite3_stmt *pStmt, int i, _GoString_ data) {
	return sqlite3_bind_text(pStmt, i, _GoStringPtr(data), _GoStringLen(data), SQLITE_STATIC);
}

int _sqlite3_step(sqlite3_stmt *pStmt, char *pBuf, int nBytes) {
	int nCol = sqlite3_column_count(pStmt);
	while (TRUE) {
		if (sqlite3_step(pStmt) != SQLITE_ROW) {
			break;
		}
		for (int i = 0; i < nCol; i++) {
			switch (sqlite3_column_type(pStmt, i)) {
				case SQLITE_INTEGER:
					break;
    			case SQLITE_TEXT:
					break;
				default:
					break;
			}
		}
	}
	return 0;
}
*/
import "C"

import (
	"errors"
	"unsafe"
)

type DB struct {
	p *C.sqlite3
}

type Stmt struct {
	p *C.sqlite3_stmt
}

func Open(URI string) (*DB, error) {
	var p *C.sqlite3
	r := C._sqlite3_open(&p, URI)
	if r != C.SQLITE_OK {
		C.sqlite3_close_v2(p)
		return nil, errors.New("cannot open database")
	}
	return &DB{p: p}, nil
}

func (d *DB) Close() {
	C.sqlite3_close_v2(d.p)
}

func (d *DB) Prepare(SQL string) (*Stmt, error) {
	var p *C.sqlite3_stmt
	r := C._sqlite3_prepare(d.p, &p, SQL)
	if r != C.SQLITE_OK {
		C.sqlite3_finalize(p)
		return nil, errors.New("cannot prepare statement")
	}
	return &Stmt{p: p}, nil
}

func (s *Stmt) Close() {
	C.sqlite3_finalize(s.p)
}

func (d *DB) Backup(URI string) error {
	r := C._sqlite3_copy(d.p, URI, 1)
	if r != C.SQLITE_OK {
		return errors.New("cannot backup database")
	}
	return nil
}

func (d *DB) Restore(URI string) error {
	r := C._sqlite3_copy(d.p, URI, 0)
	if r != C.SQLITE_OK {
		return errors.New("cannot restore database")
	}
	return nil
}

func (s *Stmt) bind(args ...interface{}) error {
	for k, v := range args {
		i := C.int(k + 1)
		r := C.int(0)
		switch v := v.(type) {
		case int:
			r = C.sqlite3_bind_int(s.p, i, C.int(v))
		case string:
			r = C._sqlite3_bind_text_static(s.p, i, v)
		default:
			return errors.New("cannot bind parameters")
		}
		if r != C.SQLITE_OK {
			return errors.New("cannot bind parameters")
		}
	}
	return nil
}

func (s *Stmt) Exec(args ...interface{}) error {
	r := C.sqlite3_reset(s.p)
	if r != C.SQLITE_OK {
		return errors.New("cannot reset statement")
	}
	err := s.bind(args...)
	if err != nil {
		return err
	}
	r = C.sqlite3_step(s.p)
	if r == C.SQLITE_DONE {
		return nil
	}
	if r == C.SQLITE_ROW {
		return nil
	}
	return errors.New("cannot execute statement")
}

const fetchBufferSize = 4096

func (s *Stmt) next() int {
	var zBuff *C.char
	var nBytes C.int
	buf := make([]byte, fetchBufferSize)
	zBuff = (*C.char)(unsafe.Pointer(&buf[0]))
	nBytes = C.int(fetchBufferSize)
	C._sqlite3_step(s.p, zBuff, nBytes)
	return 0
}

func (s *Stmt) Next() bool {
	return false
}

func (s *Stmt) Scan(args ...interface{}) error {
	return nil
}
