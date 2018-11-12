#include <stdio.h>
#include <string.h>
#include <sqlite3.h>

// replace [_GoString_ ] to [const char *]
// replace [_GoStringPtr] to [(char *)]
// replace [_GoStringLen] to [strlen]

#define URI_MAX_SIZE 256

int _sqlite3_open(sqlite3 **ppDb, const char *URI) {
	size_t size = strlen(URI);
	if (size >= URI_MAX_SIZE) {
		return SQLITE_ERROR;
	}
	char cURI[URI_MAX_SIZE];
	memcpy(cURI, (char *)(URI), size);
	cURI[size] = 0;
	return sqlite3_open(cURI, ppDb);
}

int _sqlite3_copy(sqlite3 *pDb, const char *URI, int isSave) {
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

int _sqlite3_prepare(sqlite3 *pDb, sqlite3_stmt **ppStmt, const char *SQL) {
	return sqlite3_prepare_v2(pDb, (char *)(SQL), strlen(SQL), ppStmt, NULL);
}

int _sqlite3_bind_text_static(sqlite3_stmt *pStmt, int i, const char *data) {
	return sqlite3_bind_text(pStmt, i, (char *)(data), strlen(data), SQLITE_STATIC);
}

int main(int argc, char* argv[]) {
	int rc = 0;
	sqlite3 *pDb;
	sqlite3_stmt *pStmt;

	rc = _sqlite3_open(&pDb, ":memory:");
	if (rc != SQLITE_OK) {
		puts("cannot open database");
		goto finish;
	}

	rc = _sqlite3_copy(pDb, "file:index.db?mode=ro", 0);
	if (rc != SQLITE_OK) {
		puts("cannot restore database");
		goto finish;
	}

	//rc = _sqlite3_prepare(pDb, &pStmt, "SELECT rowid + 19, 'test', rowid, 'test', rowid + 19, 'test', rowid, 'test', rowid + 19, 'test', rowid, 'test', rowid + 19, 'test', rowid, 'test' FROM CatalogFTS5('(B1290 OR B3242 OR B1031) AND S42')");
	rc = _sqlite3_prepare(pDb, &pStmt, "SELECT rowid FROM CatalogFTS5('(B1290 OR B3242 OR B1031) AND S42')");
	if (rc != SQLITE_OK) {
		puts("cannot prepare statement");
		goto finish;
	}

	//int nc = sqlite3_column_count(pStmt);

	for (int i = 0; i < 10000; i++) {
		rc = sqlite3_reset(pStmt);
		if (rc != SQLITE_OK) {
			puts("cannot reset statement");
			goto finish;
		}
		while (1) {
			rc = sqlite3_step(pStmt);
			if (rc != SQLITE_ROW) {
				break;
			}
			//for (int c = 0; c < nc; c++) {
			//	int ct = sqlite3_column_type(pStmt, c) & 0xFF;
			//	if ((ct != SQLITE_INTEGER) && (ct != SQLITE_TEXT)) {
			//		ct = SQLITE_NULL;
			//	}
			//	// write type = 1 bytex
			//	// int = 8 byte
			//	// string = 4 bytes size, N byte value
			//}
		}
	}

finish:
	sqlite3_finalize(pStmt);
	sqlite3_close_v2(pDb);
	return rc;
}
