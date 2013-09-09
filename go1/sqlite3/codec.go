// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

import "C"

import "unsafe"

// CodecFunc is a callback function invoked by SQLite when a key is specified
// for an attached database. It returns the Codec implementation that is used to
// encode and decode all database and journal pages. The codec is disabled if
// nil is returned.
type CodecFunc func(file, name string, pageSize, reserve int, key []byte) Codec

// Codec is the interface used to encode/decode database and journal pages as
// they are written to and read from the disk.
//
// The op value passed to Encode and Decode methods identifies the operation
// being performed. It is undocumented and changed meanings over time (the codec
// API was introduced in 2004), but is believed to be a bitmask of the following
// values:
//
// 	1 = journal, not set for WAL, always set when decoding
// 	2 = disk I/O, always set
// 	4 = encode
//
// Thus, op is always 3 when decoding, 6 when encoding for the database file or
// the WAL, and 7 when encoding for the journal.
type Codec interface {
	// Reserve returns the number of bytes that should be reserved for the codec
	// at the end of each page. Returning -1 indicates that the current value,
	// which was provided to CodecFunc, does not need to be changed. Each page
	// must have at least 480 usable bytes (i.e. the codec cannot reserve more
	// than 32 bytes if the page size is 512).
	Reserve() int

	// Resize is called when the codec is first attached to the pager and for
	// all subsequent page size changes. It can be used to allocate the encode
	// buffer.
	Resize(pageSize, reserve int)

	// Encode returns an encoded copy of a page or nil to indicate an error. The
	// original page cannot be modified and may be returned without making a
	// copy. The codec is allowed to reuse a single buffer of identical size for
	// encoding. Bytes 16 through 23 of page 1 may not be altered.
	Encode(page []byte, pageNum uint32, op int) []byte

	// Decode performs an in-place decoding of a single page. It returns true on
	// success and false on error, which will be interpreted by SQLite as a
	// NOMEM condition.
	Decode(page []byte, pageNum uint32, op int) bool

	// Key returns the original key that was used to initialize the codec.
	Key() []byte

	// FastRekey returns true if the codec can change the database key by
	// updating just the first page.
	FastRekey() bool

	// Free releases codec resources when the pager is destroyed or when the
	// codec attachment fails.
	Free()
}

// codecs keeps a reference to all active codec wrappers to prevent them from
// being garbage collected.
var codecs = make(map[*codec]struct{})

// codec is a wrapper around the actual Codec interface. It keeps track of the
// current page size in order to convert page pointers into byte slices.
type codec struct {
	Codec
	pageSize C.int
}

//export go_codec_init
func go_codec_init(db unsafe.Pointer, zFilename, zName *C.char,
	nBuf, nRes C.int, pKey unsafe.Pointer, nKey C.int, nNewRes *C.int,
) unsafe.Pointer {
	if c := dbToConn[db]; c != nil && c.db != nil && c.codec != nil {
		file := C.GoString(zFilename)
		name := C.GoString(zName)
		key := C.GoBytes(pKey, nKey)
		if ci := c.codec(file, name, int(nBuf), int(nRes), key); ci != nil {
			cs := &codec{ci, nBuf}
			codecs[cs] = struct{}{}
			*nNewRes = C.int(ci.Reserve())
			return unsafe.Pointer(cs)
		}
	}
	return nil
}

//export go_codec_exec
func go_codec_exec(pCodec, pData unsafe.Pointer, pgno uint32, op C.int) unsafe.Pointer {
	cs := (*codec)(pCodec)
	if page := goBytes(pData, cs.pageSize); op&4 == 1 {
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
	delete(codecs, cs)
	cs.Free()
	cs.Codec = nil
}
