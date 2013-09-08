// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3

import "C"

import "unsafe"

// codecs keeps a reference to all active codec wrappers to prevent them from
// being garbage collected.
var codecs = make(map[*codec]struct{})

// CodecFunc is a callback function invoked by SQLite when a key is specified
// for an attached database. It returns the Codec implementation that is used to
// encode and decode all database and journal pages. The codec is disabled if
// nil is returned.
type CodecFunc func(file, name string, pageSize, reserve int, key []byte) Codec

// Codec is the interface used to encode/decode database and journal pages as
// they are written to and read from the disk.
type Codec interface {
	// Reserve returns the number of bytes that should be reserved for the codec
	// at the end of each page. Returning -1 indicates that the current value,
	// which was provided to CodecFunc, does not need to be changed.
	Reserve() int

	// Encode returns an encoded copy of a page. The original page must not be
	// modified. The codec is allowed to reuse a single buffer to perform the
	// encoding or to return the original page without copying it. The page size
	// cannot change and bytes 16 through 23 of page 1 may not be altered. The
	// op value isn't completely understood at the moment.
	Encode(page []byte, pageNum uint32, op int) []byte

	// Decode performs an in-place decoding of a single page.
	Decode(page []byte, pageNum uint32, op int)

	// Resize is called when the codec is first attached to the pager and for
	// all subsequent page size changes. It can be used to allocate the encode
	// buffer.
	Resize(pageSize, reserve int)

	// Key returns the original key that was used to initialize the codec.
	Key() []byte

	// FastRekey returns true if the codec can change the database key by
	// updating just the first page.
	FastRekey() bool

	// Free releases codec resources when the pager is destroyed or when the
	// codec attachment fails.
	Free()
}

// codec is a wrapper around the actual codec interface. It keeps track of the
// current page size in order to convert page pointers into byte slices.
type codec struct {
	Codec
	pageSize C.int
}

//export go_codec_init
func go_codec_init(db unsafe.Pointer, zFilename, zName *C.char,
	nBuf, nRes C.int, pKey unsafe.Pointer, nKey C.int, nNewRes *C.int,
) unsafe.Pointer {
	c := dbToConn[db]
	if c == nil || c.db == nil || c.codec == nil {
		return nil
	}
	file := C.GoString(zFilename)
	name := C.GoString(zName)
	key := C.GoBytes(pKey, nKey)
	if ci := c.codec(file, name, int(nBuf), int(nRes), key); ci != nil {
		cs := &codec{ci, nBuf}
		codecs[cs] = struct{}{}
		*nNewRes = C.int(ci.Reserve())
		return unsafe.Pointer(cs)
	}
	return nil
}

//export go_codec_exec
func go_codec_exec(pCodec, pData unsafe.Pointer, pgno uint32, op C.int) unsafe.Pointer {
	cs := (*codec)(pCodec)
	// TODO(max): Is op a bitmask or a value?
	if op&4 == 0 {
		cs.Decode(goBytes(pData, cs.pageSize), pgno, int(op))
		return pData
	}
	return cBytes(cs.Encode(goBytes(pData, cs.pageSize), pgno, int(op)))
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
