// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#if defined(SQLITE_AMALGAMATION) && defined(SQLITE_HAS_CODEC)

// codec.go exports.
int go_codec_init(void*,const char*,const char*,int,int,const void*,int,void**,int*);
void *go_codec_exec(void*,void*,Pgno,int);
void go_codec_resize(void*,int,int);
void go_codec_get_key(void*,void**,int*);
void go_codec_free(void*);

int sqlite3CodecAttach(sqlite3*,int,const void*,int);
void sqlite3CodecGetKey(sqlite3*,int,void**,int*);

// sqlite3_key sets the codec key for the main database.
SQLITE_API int sqlite3_key(sqlite3 *db, const void *pKey, int nKey) {
	return sqlite3_key_v2(db, 0, pKey, nKey);
}

// sqlite3_key_v2 sets the codec key for the specified database.
SQLITE_API int sqlite3_key_v2(sqlite3 *db, const char *zDbName, const void *pKey, int nKey) {
	int iDb = 0;
	int rc = SQLITE_OK;
	sqlite3_mutex_enter(db->mutex);
	if (zDbName) {
		iDb = sqlite3FindDbName(db, zDbName);
	}
	if (iDb >= 0) {
		rc = sqlite3CodecAttach(db, iDb, pKey, nKey);
	} else {
		rc = SQLITE_ERROR;
		sqlite3Error(db, rc, "unknown database %s", zDbName);
	}
	rc = sqlite3ApiExit(db, rc);
	sqlite3_mutex_leave(db->mutex);
	return rc;
}

// sqlite3_rekey changes the codec key for the main database.
SQLITE_API int sqlite3_rekey(sqlite3 *db, const void *pKey, int nKey) {
	return sqlite3_rekey_v2(db, 0, pKey, nKey);
}

// sqlite3_rekey_v2 changes the codec key for the specified database.
SQLITE_API int sqlite3_rekey_v2(sqlite3 *db, const char *zDbName, const void *pKey, int nKey) {
	int rc = SQLITE_ERROR;
	sqlite3_mutex_enter(db->mutex);
	// TODO(max): Implement rekey.
	sqlite3Error(db, rc, "rekey is not implemented");
	rc = sqlite3ApiExit(db, rc);
	sqlite3_mutex_leave(db->mutex);
	return rc;
}

// sqlite3_activate_see isn't used by Go codecs, but it needs to be linked in.
SQLITE_API void sqlite3_activate_see(const char *zPassPhrase) {}

// sqlite3CodecAttach configures the reserved space at the end of each page and
// attaches the database codec.
int sqlite3CodecAttach(sqlite3 *db, int iDb, const void *pKey, int nKey) {
	int rc = SQLITE_OK;
	Btree *pBt = db->aDb[iDb].pBt;
	Pager *pPager = sqlite3BtreePager(pBt);
	const char *zPath;
	const char *zName;
	int nBuf;
	int nRes;
	void *pCodec = 0;
	int nNewRes = 0;

	if (pPager->memDb) {
		return rc; // SQLite doesn't allow codecs for in-memory databases
	}

	zPath = sqlite3BtreeGetFilename(pBt);
	zName = db->aDb[iDb].zName;
	nBuf = sqlite3BtreeGetPageSize(pBt);
	nRes = sqlite3BtreeGetReserve(pBt);
	rc = go_codec_init(db, zPath, zName, nBuf, nRes, pKey, nKey, &pCodec, &nNewRes);

	if (pCodec != 0) {
		if (rc != SQLITE_OK) {
			go_codec_free(pCodec);
			return rc;
		}
		if (nNewRes >= 0 && nNewRes != nRes) {
			rc = sqlite3BtreeSetPageSize(pBt, -1, nNewRes, 0);
			if (rc != SQLITE_OK) {
				go_codec_free(pCodec);
				return rc;
			}
		}
		sqlite3PagerSetCodec(pPager, go_codec_exec, go_codec_resize, go_codec_free, pCodec);
	}
	return rc;
}

// sqlite3CodecGetKey returns the codec key for the specified database.
void sqlite3CodecGetKey(sqlite3 *db, int iDb, void **pKey, int *nKey) {
	void *pCodec = sqlite3PagerGetCodec(sqlite3BtreePager(db->aDb[iDb].pBt));
	*pKey = 0;
	*nKey = 0;
	if (pCodec) {
		go_codec_get_key(pCodec, pKey, nKey);
	}
}

#endif
