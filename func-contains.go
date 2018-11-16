package sqlite3

/*
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

typedef unsigned char byte;

static void contains(sqlite3_context *context, int argc, sqlite3_value **argv) {
    int value = sqlite3_value_int(argv[1]);
    const int value_size = sizeof(value);
    byte bytes[value_size];
    for (int i = 0; i < value_size; ++i) {
        bytes[i] = value >> (i * 8);
    }
	int blob_size = sqlite3_value_bytes(argv[0]);
	byte *blob = (byte *)sqlite3_value_blob(argv[0]);
	for (int i = 0; i < blob_size / value_size; ++i) {
        int offset = i * value_size;
        if (memcmp(bytes, &blob[offset], value_size) == 0) {
        	sqlite3_result_int(context, i + 1);
            return;
        }
    }
    sqlite3_result_int(context, 0);
}

int sqlite3_register_blob_contains_func(sqlite3 *db) {
	return sqlite3_create_function_v2(db, "contains", 2, SQLITE_UTF8, NULL, contains, NULL, NULL, NULL);
}
*/
import "C"
import "errors"

func (d *DB) RegisterContainsFunc() error {
	r := C.sqlite3_register_blob_contains_func(d.p)
	if r != C.SQLITE_OK {
		return errors.New("cannot register function")
	}
	return nil
}
