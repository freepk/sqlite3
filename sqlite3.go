package sqlite3

/*
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

int sqlite3_open_x(const char *pURI, int nURI, sqlite3 **ppDb) {
	char cURI[512];
	if (nURI >= sizeof(cURI)) {
		return SQLITE_ERROR;
	}
	memcpy(cURI, pURI, nURI);
	cURI[nURI] = 0;
	return sqlite3_open(cURI, ppDb);
}

int sqlite3_bind_text_x(sqlite3_stmt *pStmt, int iCol, const char *pSQL, int nSQL) {
	return sqlite3_bind_text(pStmt, iCol, pSQL, nSQL, SQLITE_STATIC);
}

int sqlite3_copy(sqlite3 *pDb, const char *pURI, int nURI, int isSave) {
	sqlite3 *pTo;
	sqlite3 *pFrom;
	sqlite3 *pTemp;
	int rc = sqlite3_open_x(pURI, nURI, &pTemp);
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

int sqlite3_encode_int64(char *pBuf, int nBuf, int64_t iVal) {
	int r = 1 + 8;
	if (r > nBuf) {
		return 0;
	}
	*(int8_t*)pBuf = SQLITE_INTEGER;
	*(int64_t*)&pBuf[1] = iVal;
	return r;
}

int sqlite3_encode_text(char *pBuf, int nBuf, const unsigned char *pVal, int nVal) {
	int r = 1 + 4 + nVal;
	if (r > nBuf) {
		return 0;
	}
	*(int8_t*)pBuf = SQLITE_TEXT;
	*(int32_t*)&pBuf[1] = nVal;
	memcpy(&pBuf[5], pVal, nVal);
	return r;
}

int sqlite3_encode_null(char *pBuf, int nBuf) {
	int r = 1;
	if (r > nBuf) {
		return 0;
	}
	*(int8_t*)pBuf = SQLITE_NULL;
	return r;
}

int sqlite3_encode_row(sqlite3_stmt *pStmt, char *pBuf, int nBuf) {
	int c = sqlite3_column_count(pStmt);
	int r = 4;
	if (r > nBuf) {
		return 0;
	}
	for (int i = 0; i < c; i++) {
		int n = 0;
		switch(sqlite3_column_type(pStmt, i)) {
			case SQLITE_INTEGER:
				n = sqlite3_encode_int64(pBuf + r, nBuf - r,
					sqlite3_column_int64(pStmt, i));
				break;
			case SQLITE_TEXT:
				n = sqlite3_encode_text(pBuf + r, nBuf - r,
					sqlite3_column_text(pStmt, i),
					sqlite3_column_bytes(pStmt, i));
				break;
			default:
				n = sqlite3_encode_null(pBuf + r, nBuf - r);
		}
		if (n == 0) {
			return 0;
		}
		r += n;
	}
	*(int32_t*)pBuf = r;
	return r;
}

int sqlite3_prefetch(sqlite3_stmt *pStmt, char *pBuf, int nBuf) {
	int r = 0;
	while (1) {
		int n = sqlite3_encode_row(pStmt, pBuf + r, nBuf - r);
		if (n == 0) {
			return r;
		}
		int s = sqlite3_step(pStmt);
		if (s != SQLITE_DONE && s != SQLITE_ROW) {
			break;
		}
		r += n;
	}
	return r;
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

func cString(s string) (*C.char, C.int) {
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(h.Data)), C.int(h.Len)
}

func cSlice(b []byte) (*C.char, C.int) {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return (*C.char)(unsafe.Pointer(h.Data)), C.int(h.Len)
}

func Open(URI string) (*DB, error) {
	var p *C.sqlite3
	z, n := cString(URI)
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
	z, n := cString(URI)
	r := C.sqlite3_copy(d.p, z, n, 1)
	if r != C.SQLITE_OK {
		return errors.New("cannot backup database")
	}
	return nil
}

func (d *DB) Restore(URI string) error {
	z, n := cString(URI)
	r := C.sqlite3_copy(d.p, z, n, 0)
	if r != C.SQLITE_OK {
		return errors.New("cannot restore database")
	}
	return nil
}

func (d *DB) Prepare(SQL string) (*Stmt, error) {
	var p *C.sqlite3_stmt
	z, n := cString(SQL)
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
			z, n := cString(v)
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
	if r == C.SQLITE_DONE || r == C.SQLITE_ROW {
		return nil
	}
	return errors.New("cannot execute statement")
}

func (s *Stmt) prefetch() bool {
	if s.b == nil {
		s.b = byteBufPool.Get().([]byte)
	}
	z, n := cSlice(s.b)
	r := C.sqlite3_prefetch(s.p, z, n)
	s.r = s.b[:r]
	return r > 0
}

func (s *Stmt) hasRow() bool {
	return len(s.r) > 0
}

func (s *Stmt) rowSize() int {
	return int(*(*int32)(unsafe.Pointer(&s.r[0])))
}

func (s *Stmt) Next() bool {
	if !s.hasRow() {
		return s.prefetch()
	}
	n := s.rowSize()
	s.r = s.r[n:]
	if !s.hasRow() {
		return s.prefetch()
	}
	return true
}

func (s *Stmt) RowBytes() []byte {
	if s.hasRow() {
		n := s.rowSize()
		return s.r[:n]
	}
	return nil
}
