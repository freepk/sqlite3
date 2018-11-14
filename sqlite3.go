package sqlite3

/*

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

int sqlite3_open_x(const char *zURI, int nBytes, sqlite3 **ppDb) {
	char cURI[512];
	if (nBytes >= sizeof(cURI)) {
		return SQLITE_ERROR;
	}
	memcpy(cURI, zURI, nBytes);
	cURI[nBytes] = 0;
	return sqlite3_open(cURI, ppDb);
}

int sqlite3_bind_text_x(sqlite3_stmt *pStmt, int iCol, const char *zSQL, int nBytes) {
	return sqlite3_bind_text(pStmt, iCol, zSQL, nBytes, SQLITE_STATIC);
}

int sqlite3_copy(sqlite3 *pDb, const char *zURI, int nBytes, int isSave) {
	sqlite3 *pTo;
	sqlite3 *pFrom;
	sqlite3 *pTemp;
	int rc = sqlite3_open_x(zURI, nBytes, &pTemp);
	if (rc != SQLITE_OK) {
		sqlite3_close_v2(pTemp);
		return rc;
	}
	pTo = (isSave ? pTemp : pDb);
	pFrom = (isSave ? pDb : pTemp);
	sqlite3_backup *pBackup = sqlite3_backup_init(pTo, "main", pFrom, "main");
	if (pBackup) {
		sqlite3_backup_step(pBackup, -1);
		sqlite3_backup_finish(pBackup);
	}
	rc = sqlite3_errcode(pTo);
	sqlite3_close_v2(pTemp);
	return rc;
}

#define SQLITE_ENC1(a,b,c,d) \
	if ((b) < 1) return 0; \
	*(uint8_t *)(a) = (c); \
	(a) += 1; (b) -= 1; (d) += 1;

#define SQLITE_ENC2(a,b,c,d) \
	if ((b) < 2) return 0; \
	*(uint16_t *)(a) = (c); \
	(a) += 2; (b) -= 2; (d) += 2;

#define SQLITE_ENC4(a,b,c,d) \
	if ((b) < 4) return 0; \
	*(uint32_t *)(a) = (c); \
	(a) += 4; (b) -= 4; (d) += 4;

#define SQLITE_ENC8(a,b,c,d) \
	if ((b) < 8) return 0; \
	*(uint64_t *)(a) = (c); \
	(a) += 8; (b) -= 8; (d) += 8;

#define SQLITE_ENCN(a,b,c,d,e) \
	if ((b) < (d)) return 0; \
	memcpy(a, c, d); \
	(a) += d; (b) -= d; (e) += d;

int sqlite3_write_columns(sqlite3_stmt *pStmt, int iCol, void *pBuf, int nBytes) {
	int n = 0;
	int m = sqlite3_column_count(pStmt);
	SQLITE_ENC4(pBuf, nBytes, 0, n);
	for (int i = 0; i < m; i++) {
		switch(sqlite3_column_type(pStmt, iCol)) {
			case SQLITE_INTEGER: {
				SQLITE_ENC1(pBuf, nBytes, SQLITE_INTEGER, n);
				SQLITE_ENC8(pBuf, nBytes, sqlite3_column_int64(pStmt, iCol), n);
				break;
			}
			case SQLITE_TEXT: {
				int b = sqlite3_column_bytes(pStmt, iCol);
				SQLITE_ENC1(pBuf, nBytes, SQLITE_TEXT, n);
				SQLITE_ENC4(pBuf, nBytes, b, n);
				SQLITE_ENCN(pBuf, nBytes, sqlite3_column_text(pStmt, iCol), b, n);
				break;
			}
			default: {
				SQLITE_ENC1(pBuf, nBytes, SQLITE_NULL, n);
				break;
			}
		}
	}
	return n;
}

*/
import "C"

import (
	"errors"
	"reflect"
	"sync"
	"unsafe"
)

var byteBufPool = sync.Pool{New: func() interface{} {
	return make([]byte, 8192)
}}

type DB struct {
	p *C.sqlite3
}

type Stmt struct {
	p *C.sqlite3_stmt
	b []byte
	r []byte
}

func cStr(s string) (*C.char, C.int) {
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(h.Data)), C.int(h.Len)
}

func Open(URI string) (*DB, error) {
	var p *C.sqlite3
	z, n := cStr(URI)
	r := C.sqlite3_open_x(z, n, &p)
	if r != C.SQLITE_OK {
		C.sqlite3_close_v2(p)
		return nil, errors.New("cannot open database")
	}
	return &DB{p: p}, nil
}

func (d *DB) Close() {
	C.sqlite3_close_v2(d.p)
}

func (d *DB) Backup(URI string) error {
	z, n := cStr(URI)
	r := C.sqlite3_copy(d.p, z, n, 1)
	if r != C.SQLITE_OK {
		return errors.New("cannot backup database")
	}
	return nil
}

func (d *DB) Restore(URI string) error {
	z, n := cStr(URI)
	r := C.sqlite3_copy(d.p, z, n, 0)
	if r != C.SQLITE_OK {
		return errors.New("cannot restore database")
	}
	return nil
}

func (d *DB) Prepare(SQL string) (*Stmt, error) {
	var p *C.sqlite3_stmt
	z, n := cStr(SQL)
	r := C.sqlite3_prepare_v2(d.p, z, n, &p, nil)
	if r != C.SQLITE_OK {
		return nil, errors.New("cannot prepare statement")
	}
	return &Stmt{p: p}, nil
}

func (s *Stmt) Close() {
	if s.b != nil {
		byteBufPool.Put(s.b)
		s.b = nil
		s.r = nil
	}
	C.sqlite3_finalize(s.p)
}

func (s *Stmt) bind(args ...interface{}) error {
	for k, v := range args {
		i := C.int(k + 1)
		r := C.int(0)
		switch v := v.(type) {
		case int:
			r = C.sqlite3_bind_int64(s.p, i, C.sqlite3_int64(v))
		case string:
			z, n := cStr(v)
			r = C.sqlite3_bind_text_x(s.p, i, z, n)
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

func (s *Stmt) prefetch() {
	if s.b == nil {
		s.b = byteBufPool.Get().([]byte)
	}
	s.r = s.b[:0]
}
