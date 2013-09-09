// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

import "C"

import (
	"bytes"
	"sync"
	"unsafe"
)

// CodecFunc is a callback function invoked by SQLite when a key is specified
// for an attached database. It returns the Codec implementation that will be
// used to encode and decode all database and journal pages. Returning (nil, OK)
// disables the codec. A result code other than OK indicates an error.
type CodecFunc func(di DbInfo, key []byte) (ci Codec, rc int)

// DbInfo describes the database to which a codec is being attached.
type DbInfo interface {
	Path() string  // Full path to the database file
	Name() string  // Database name as it is known to SQLite (e.g. "main")
	PageSize() int // Current page size
	Reserve() int  // Current number of bytes reserved in each page
}

// Codec is the interface used to encode/decode database and journal pages as
// they are written to and read from the disk.
//
// The op value passed to Encode and Decode methods identifies the operation
// being performed. It is undocumented and changed meanings over time (the codec
// API was introduced in 2004), but is believed to be a bitmask of the following
// values:
//
// 	1 = journal page, not set for WAL, always set when decoding
// 	2 = disk I/O, always set
// 	4 = encode
//
// In the current implementation, op is always 3 when decoding, 6 when encoding
// for the database file or the WAL, and 7 when encoding for the journal.
type Codec interface {
	// Reserve returns the number of bytes that should be reserved for the codec
	// at the end of each page. The upper limit is 255 (32 if the page size is
	// 512). Returning -1 leaves the current value unchanged.
	Reserve() int

	// Resize is called when the codec is first attached to the pager and for
	// all subsequent page size changes. It can be used to allocate the encode
	// buffer.
	Resize(pageSize, reserve int)

	// Encode returns an encoded copy of a page or nil to indicate an error. The
	// original page may be returned without making a copy, but it must never be
	// modified. The codec is allowed to reuse a single buffer of identical size
	// for encoding. Bytes 16 through 23 of page 1 cannot be encoded.
	Encode(page []byte, pageNum uint32, op int) []byte

	// Decode decodes the page in-place. Bytes 16 through 23 of page 1 must be
	// left at their original values. It returns true on success and false on
	// error, which will be interpreted by SQLite as a NOMEM condition.
	Decode(page []byte, pageNum uint32, op int) bool

	// Key returns the original key that was used to initialize the codec. Some
	// implementations may be better off returning nil or a fake value. Search
	// lib/sqlite3.c for "sqlite3CodecGetKey" to see how the key is used.
	Key() []byte

	// FastRekey returns true if the codec can change the database key by
	// updating just the first page.
	FastRekey() bool

	// Free releases codec resources when the pager is destroyed or when the
	// codec attachment fails.
	Free()
}

// Codec registry.
var (
	codecReg map[string]CodecFunc
	codecMu  sync.Mutex
)

// RegisterCodec associates CodecFunc f with the given key prefix. Connections
// using the default codec handler will call f when a key is provided in the
// format "<keyPrefix>:<...>".
func RegisterCodec(keyPrefix string, f CodecFunc) {
	codecMu.Lock()
	defer codecMu.Unlock()
	if codecReg != nil {
		codecReg[keyPrefix] = f
	} else {
		codecReg = map[string]CodecFunc{keyPrefix: f}
	}
}

// defaultCodecFunc is used by all new connections to select a codec constructor
// from the registry based on the key prefix.
func defaultCodecFunc(di DbInfo, key []byte) (ci Codec, rc int) {
	i := bytes.IndexByte(key, ':')
	if i == -1 {
		i = 0
	}
	if f := getCodecFunc(bstr(key[:i])); f != nil {
		return f(di, key)
	}
	return nil, ERROR
}

// getCodecFunc returns the CodecFunc for the given key prefix.
func getCodecFunc(keyPrefix string) CodecFunc {
	codecMu.Lock()
	defer codecMu.Unlock()
	if codecReg != nil {
		return codecReg[keyPrefix]
	}
	return nil
}

// codecState keeps a reference to all active codec wrappers to prevent them
// from being garbage collected.
var codecState = make(map[*codec]struct{})

// codec is a wrapper around the actual Codec interface. It keeps track of the
// current page size in order to convert page pointers into byte slices.
type codec struct {
	Codec
	pageSize C.int
}

// dbInfo is the default DbInfo implementation.
type dbInfo struct {
	zPath    *C.char
	zName    *C.char
	path     string
	name     string
	pageSize int
	reserve  int
}

func (di *dbInfo) Path() string {
	if di.zPath != nil {
		di.path = C.GoString(di.zPath)
		di.zPath = nil
	}
	return di.path
}

func (di *dbInfo) Name() string {
	if di.zName != nil {
		di.name = C.GoString(di.zName)
		di.zName = nil
	}
	return di.name
}

func (di *dbInfo) PageSize() int { return di.pageSize }
func (di *dbInfo) Reserve() int  { return di.reserve }

//export go_codec_init
func go_codec_init(db unsafe.Pointer, zPath, zName *C.char, nBuf, nRes C.int,
	pKey unsafe.Pointer, nKey C.int, pCodec *unsafe.Pointer, nNewRes *C.int,
) C.int {
	c := dbToConn[db]
	if c == nil || c.db == nil || c.codec == nil {
		return OK
	}
	di := &dbInfo{
		zPath:    zPath,
		zName:    zName,
		pageSize: int(nBuf),
		reserve:  int(nRes),
	}
	var key []byte
	if nKey > 0 {
		key = C.GoBytes(pKey, nKey)
	}
	ci, rc := c.codec(di, key)
	if ci != nil {
		cs := &codec{ci, nBuf}
		codecState[cs] = struct{}{}
		*pCodec = unsafe.Pointer(cs)
		*nNewRes = C.int(ci.Reserve())
	}
	*di = dbInfo{}
	return C.int(rc)
}

//export go_codec_exec
func go_codec_exec(pCodec, pData unsafe.Pointer, pgno uint32, op C.int) unsafe.Pointer {
	cs := (*codec)(pCodec)
	if page := goBytes(pData, cs.pageSize); op&4 != 0 {
		return cBytes(cs.Encode(page, pgno, int(op)))
	} else if cs.Decode(page, pgno, int(op)) {
		return pData
	}
	return nil
}

//export go_codec_resize
func go_codec_resize(pCodec unsafe.Pointer, nBuf, nRes C.int) {
	cs := (*codec)(pCodec)
	cs.pageSize = nBuf
	cs.Resize(int(nBuf), int(nRes))
}

//export go_codec_get_key
func go_codec_get_key(pCodec unsafe.Pointer, pKey *unsafe.Pointer, nKey *C.int) {
	if key := (*codec)(pCodec).Key(); len(key) > 0 {
		*pKey = cBytes(key)
		*nKey = C.int(len(key))
	}
}

//export go_codec_free
func go_codec_free(pCodec unsafe.Pointer) {
	cs := (*codec)(pCodec)
	delete(codecState, cs)
	cs.Free()
	cs.Codec = nil
}
