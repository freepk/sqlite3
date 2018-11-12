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
			sqlite3_backup_step(pBackup, -1);
			sqlite3_backup_finish(pBackup);
		}
		rc = sqlite3_errcode(pTo);
	}
	sqlite3_close(pTemp);
	return rc;
}

int _sqlite3_prepare(sqlite3 *pDb, sqlite3_stmt **ppStmt, _GoString_ SQL) {
	return sqlite3_prepare_v2(pDb, _GoStringPtr(SQL), _GoStringLen(SQL), ppStmt, NULL);
}

int _sqlite3_bind_text_static(sqlite3_stmt *pStmt, int iCol, _GoString_ data) {
	return sqlite3_bind_text(pStmt, iCol, _GoStringPtr(data), _GoStringLen(data), SQLITE_STATIC);
}

int _sqlite3_write_int(sqlite3_stmt *pStmt, int iCol, char *pBuf, int szBuf) {
	int r = 1;
	r += 8;
	if (szBuf < r) {
		return 0;
	}
	*pBuf = (char)SQLITE_INTEGER;
	pBuf++;
	memset(pBuf, 33, 8);
	return r;
}

int _sqlite3_write_text(sqlite3_stmt *pStmt, int iCol, char *pBuf, int szBuf) {
	int n = sqlite3_column_bytes(pStmt, iCol);
	int r = 1;
	r += 4;
	r += n;
	if (szBuf < r) {
		return 0;
	}
	*pBuf = (char)SQLITE_TEXT;
	pBuf++;
	memset(pBuf, 44, 4);
	pBuf += 4;
	memset(pBuf, 55, n);
	return r;
}

int _sqlite3_write_null(sqlite3_stmt *pStmt, int iCol, char *pBuf, int szBuf) {
	int r = 1;
	if (szBuf < r) {
		return 0;
	}
	*pBuf = (char)SQLITE_NULL;
	return r;
}

int _sqlite3_write(sqlite3_stmt *pStmt, char *pBuf, int szBuf) {
	int n = sqlite3_column_count(pStmt);
	if (n == 0) {
		return 0;
	}
	int r = 4;
	if (szBuf < r) {
		return 0;
	}
	memset(pBuf, 0, r);
	for (int i = 0; i < n; i++) {
		int n = 0;
		switch(sqlite3_column_type(pStmt, i)) {
			case SQLITE_INTEGER:
				n = _sqlite3_write_int(pStmt, i, pBuf + r, szBuf - r);
				break;
			case SQLITE_TEXT:
				n = _sqlite3_write_text(pStmt, i, pBuf + r, szBuf - r);
				break;
			default:
				n = _sqlite3_write_null(pStmt, i, pBuf + r, szBuf - r);
		}
		if (n == 0) {
			return 0;
		}
		r += n;
	}
	return r;
}

int _sqlite3_step(sqlite3_stmt *pStmt, char *pBuf, int szBuf, int *isLast) {
	int r = 0;
	while (TRUE) {
		if (sqlite3_step(pStmt) != SQLITE_ROW) {
			break;
		}
		int n = _sqlite3_write(pStmt, pBuf + r, szBuf - r);
		if (n == 0) {
			return r;
		}
		r += n;
	}
	*isLast = 1;
	return r;
}
*/
import "C"

import (
	"errors"
	"fmt"
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

const fetchBufferSize = 128

func (s *Stmt) next() int {
	var zBuf *C.char
	var bufSize C.int
	var isLast C.int

	buf := make([]byte, fetchBufferSize)
	zBuf = (*C.char)(unsafe.Pointer(&buf[0]))
	bufSize = C.int(fetchBufferSize)

	r := C._sqlite3_step(s.p, zBuf, bufSize, &isLast)
	for isLast == 0 {
		r = C._sqlite3_step(s.p, zBuf, bufSize, &isLast)
		fmt.Println("fetchBuffer", r, isLast, buf[:r])
	}
	return 0
}

func (s *Stmt) Next() bool {
	return false
}

func (s *Stmt) Scan(args ...interface{}) error {
	return nil
}
