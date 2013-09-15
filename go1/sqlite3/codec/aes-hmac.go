// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"hash"

	. "code.google.com/p/go-sqlite/go1/sqlite3"
)

// TODO(max): Remove the master key.
// TODO(max): MAC the cipher spec.
// TODO(max): HMAC key should be the same size as the output.

type aesHmac struct {
	key  []byte // Key provided to newAesHmac
	p1k  []byte // Page 1 key (subslice of key)
	p1i  []byte // Page 1 HKDF info ("go-sqlite")
	buf  []byte // Page encryption buffer
	kLen int    // Cipher and HMAC key length in bytes (16, 24, or 32)
	tLen int    // Tag length in bytes (HMAC truncation)

	// Hash function and cipher chaining mode constructors
	hash func() hash.Hash
	mode func(block cipher.Block, iv []byte) cipher.Stream

	// Block cipher and HMAC hash initialized from the master key
	block cipher.Block
	hmac  hash.Hash
}

func newAesHmac(ctx *CodecCtx, key []byte) (Codec, *Error) {
	_, opts, mk := parseKey(key)
	if mk == nil {
		return nil, keyErr
	}
	c := &aesHmac{
		key:  key,
		p1k:  mk,
		p1i:  []byte("go-sqlite\x00")[:9],
		kLen: 16,
		tLen: 16,
		hash: sha1.New,
		mode: cipher.NewCTR,
	}
	if err := c.config(opts); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *aesHmac) Reserve() int {
	return c.kLen + aes.BlockSize + c.tLen
}

func (c *aesHmac) Resize(pageSize, reserve int) {
	if reserve != c.Reserve() {
		panic("sqlite3: codec reserve value mismatch")
	}
	tMax := c.hash().Size()
	c.buf = make([]byte, pageSize, pageSize-c.tLen+tMax)
}

func (c *aesHmac) Encode(p []byte, n uint32, op int) ([]byte, *Error) {
	// Generate new random IV (HKDF salt for page 1)
	iv := c.pIV(c.buf)
	if !rnd(iv) {
		return nil, prngErr
	}

	// Create the master key if encrypting page 1 for a new database
	if c.block == nil && !c.init(p, n, true) {
		return nil, codecErr
	}

	// Encrypt-then-MAC
	cipher, hmac := c.cipher(n, iv)
	cipher.XORKeyStream(c.buf, c.pText(p))
	if n == 1 {
		copy(c.buf[16:], p[16:24]) // Bytes 16 through 23 cannot be encrypted
	}
	c.mac(c.buf, n, hmac, false)
	return c.buf, nil
}

func (c *aesHmac) Decode(p []byte, n uint32, op int) *Error {
	// Verify tag
	cipher, hmac := c.cipher(n, c.pIV(p))
	if !c.mac(p, n, hmac, true) {
		return codecErr
	}

	// Decrypt
	if n == 1 {
		copy(c.buf, p[16:24])
	}
	cipher.XORKeyStream(p, c.pText(p))
	if n == 1 {
		copy(p[16:24], c.buf)
	}

	// Get the master key if decrypting page 1 for the first time
	if c.block == nil && !c.init(p, n, false) {
		return codecErr
	}
	return nil
}

func (c *aesHmac) Key() []byte {
	return c.key
}

func (c *aesHmac) Free() {
	wipe(c.key)
	*c = aesHmac{}
}

// config applies the codec options that were provided in the key.
func (c *aesHmac) config(opts map[string]string) *Error {
	for k := range opts {
		switch k {
		case "192":
			c.kLen = 24
		case "256":
			c.kLen = 32
		case "ofb":
			c.mode = cipher.NewOFB
		case "sha256":
			c.hash = sha256.New
		default:
			return NewError(MISUSE, "invalid codec option: "+k)
		}
	}
	return nil
}

// cipher returns the stream cipher and HMAC hash for page n. Both are reset to
// their initial state.
func (c *aesHmac) cipher(n uint32, iv []byte) (cipher.Stream, hash.Hash) {
	if n > 1 {
		c.hmac.Reset()
		return c.mode(c.block, iv), c.hmac
	}

	// Derive page 1 cipher key, HMAC key, and IV
	dkLen := 2 * c.kLen
	dk := hkdf(c.p1k, iv, dkLen+aes.BlockSize, c.hash)(c.p1i)
	dk, iv = dk[:dkLen], dk[dkLen:]
	defer wipe(dk)

	// Initialize the stream cipher and HMAC for page 1
	if block, hmac := c.rekey(dk[:c.kLen], dk[c.kLen:]); block != nil {
		return c.mode(block, iv), hmac
	}
	return nil, nil
}

// mac calculates and optionally verifies the HMAC tag for page p. It returns
// true iff the tag verification is successful.
func (c *aesHmac) mac(p []byte, n uint32, h hash.Hash, verify bool) bool {
	h.Write([]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
	h.Write(c.pAuth(p))
	tag := c.pTag(c.buf)
	h.Sum(tag[:0])
	return verify && hmac.Equal(tag, c.pTag(p))
}

// init initializes the block cipher and HMAC hash using the page 1 master key.
// It returns true on success and false otherwise.
func (c *aesHmac) init(p []byte, n uint32, newKey bool) bool {
	if n != 1 {
		return false // First call to Encrypt or Decrypt must be for page 1
	}

	// Load or generate the master key
	mk := c.pKey(p)
	if newKey && !rnd(mk) {
		return false
	}

	// Derive cipher and HMAC keys from the master key
	hdr := append(make([]byte, 0, 17), p[:16]...)
	dk := hkdf(mk, nil, 2*c.kLen, c.hash)(hdr)
	defer wipe(dk)

	// Initialize the block cipher and HMAC
	c.block, c.hmac = c.rekey(dk[:c.kLen], dk[c.kLen:])
	return c.block != nil
}

// rekey creates a new block cipher and HMAC hash using the specified keys.
func (c *aesHmac) rekey(ck, hk []byte) (cipher.Block, hash.Hash) {
	if block, err := aes.NewCipher(ck); err == nil {
		return block, hmac.New(c.hash, hk)
	}
	return nil, nil
}

// pAuth returns the page subslice that gets authenticated.
func (c *aesHmac) pAuth(p []byte) []byte {
	return p[:len(p)-c.tLen]
}

// pText returns the page subslice that gets encrypted.
func (c *aesHmac) pText(p []byte) []byte {
	return p[:len(p)-c.tLen-aes.BlockSize]
}

// pKey returns the master key from page 1.
func (c *aesHmac) pKey(p []byte) []byte {
	off := len(p) - c.tLen - aes.BlockSize - c.kLen
	return p[off : off+c.kLen]
}

// pIV returns the page initialization vector (HKDF salt for page 1).
func (c *aesHmac) pIV(p []byte) []byte {
	off := len(p) - c.tLen - aes.BlockSize
	return p[off : off+aes.BlockSize]
}

// pTag returns the page authentication tag.
func (c *aesHmac) pTag(p []byte) []byte {
	return p[len(p)-c.tLen:]
}
