package sqlite3

/*
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
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

int _sqlite3_write_int64(sqlite3_stmt *pStmt, int iCol, char *pBuf, int szBuf) {
	int r = sizeof(int16_t) + sizeof(int64_t);
	if (r > szBuf) {
		return 0;
	}
	*(int16_t *)pBuf = (int16_t)SQLITE_INTEGER;
	pBuf += sizeof(int16_t);
	*(int64_t *)pBuf = (int64_t)sqlite3_column_int64(pStmt, iCol);
	return r;
}

int _sqlite3_write_text(sqlite3_stmt *pStmt, int iCol, char *pBuf, int szBuf) {
	int n = sqlite3_column_bytes(pStmt, iCol);
	int r = sizeof(int16_t) + sizeof(int32_t) + n;
	if (r > szBuf) {
		return 0;
	}
	*(int16_t *)pBuf = (int16_t)SQLITE_TEXT;
	pBuf += sizeof(int16_t);
	*(int32_t *)pBuf = (int32_t)n;
	pBuf += sizeof(int32_t);
	memcpy(pBuf, sqlite3_column_text(pStmt, iCol), n);
	return r;
}

int _sqlite3_write_null(sqlite3_stmt *pStmt, int iCol, char *pBuf, int szBuf) {
	int r = sizeof(int16_t);
	if (r > szBuf) {
		return 0;
	}
	*(int16_t *)pBuf = (int16_t)SQLITE_NULL;
	return r;
}

int _sqlite3_write(sqlite3_stmt *pStmt, char *pBuf, int szBuf) {
	int nc = sqlite3_column_count(pStmt);
	if (nc == 0) {
		return 0;
	}
	int r = sizeof(int32_t);
	if (r > szBuf) {
		return 0;
	}
	for (int i = 0; i < nc; i++) {
		int n = 0;
		switch(sqlite3_column_type(pStmt, i)) {
			case SQLITE_INTEGER:
				n = _sqlite3_write_int64(pStmt, i, pBuf + r, szBuf - r);
				break;
			case SQLITE_TEXT:
				n = _sqlite3_write_text(pStmt, i, pBuf + r, szBuf - r);
				break;
			default:
				n = _sqlite3_write_null(pStmt, i, pBuf + r, szBuf - r);
				break;
		}
		if (n == 0) {
			return 0;
		}
		r += n;
	}
	*(int32_t *)pBuf = (int32_t)r;
	return r;
}

int _sqlite3_step(sqlite3_stmt *pStmt, char *pBuf, int szBuf, int *errCode) {
	int r = 0;	
	*errCode = 0;
	while (TRUE) {
		if (sqlite3_step(pStmt) != SQLITE_ROW) {
			break;
		}
		int n = _sqlite3_write(pStmt, pBuf + r, szBuf - r);
		if (n == 0) {
			*errCode = 1;
			return r;
		}
		r += n;
	}
	return r;
}
*/
import "C"

import (
	"errors"
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
			r = C.sqlite3_bind_int64(s.p, i, C.sqlite3_int64(v))
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

func (s *Stmt) next() int {
	return 0
}

func (s *Stmt) Next() bool {
	return false
}

func (s *Stmt) Scan(args ...interface{}) error {
	return nil
}
