// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite3_test

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	. "code.google.com/p/go-sqlite/go1/sqlite3"
)

// skip causes all remaining tests to be skipped when set to true.
var skip = false

type T struct{ *testing.T }

func begin(t *testing.T) T {
	if skip {
		t.SkipNow()
	}
	return T{t}
}
func (t T) skipRestIfFailed() {
	skip = skip || t.Failed()
}
func (t T) open(name string) *Conn {
	c, err := Open(name)
	if err != nil || c == nil {
		t.Fatalf(up(1, "Open(%q) unexpected error: %v"), name, err)
	}
	return c
}
func (t T) close(c io.Closer) {
	if err := c.Close(); err != nil {
		if !t.Failed() {
			t.Fatalf(up(1, "(%T).Close() unexpected error: %v"), c, err)
		}
		t.FailNow()
	}
}

func up(frame int, s string) string {
	_, origFile, _, _ := runtime.Caller(1)
	_, frameFile, frameLine, ok := runtime.Caller(frame + 1)
	if ok && origFile == frameFile {
		return fmt.Sprintf("%d: %s", frameLine, s)
	}
	return s
}

func TestBasic(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	// Library information
	if SingleThread() {
		t.Fatalf("SQLite was built with -DSQLITE_THREADSAFE=0")
	}
	if v, min := VersionNum(), 3007017; v < min {
		t.Fatalf("VersionNum() expected >= %d; got %d", min, v)
	}

	// Setup
	c := t.open(":memory:")
	err := c.Exec(`
		CREATE TABLE t(x, y, z);
		INSERT INTO t VALUES(NULL, 123, "abc");
	`)
	if err != nil {
		t.Fatalf("c.Exec() unexpected error: %v", err)
	}

	// Query
	sql := "SELECT * FROM t"
	s, err := c.Query(sql)
	if err != nil || s == nil {
		t.Fatalf("c.Query() unexpected error: %v", err)
	}

	// Scan
	var x interface{}
	var y int
	var z string
	if err = s.Scan(&x, &y, &z); err != nil {
		t.Fatalf("s.Scan() unexpected error: %v", err)
	}
	if x != nil || y != 123 || z != "abc" {
		t.Fatalf(`s.Scan() expected nil, 123, "abc"; got %v, %d, %q`, x, y, z)
	}

	// End of scan
	if err = s.Next(); err != io.EOF {
		t.Fatalf("s.Next() expected EOF; got %v", err)
	}

	// Clean up
	t.close(s)
	t.close(c)
}

func TestInfo(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	// Connection information
	if !c.AutoCommit() {
		t.Fatalf("c.AutoCommit() expected true")
	}
	if path := c.Path("main"); path != "" {
		t.Fatalf(`c.Path("main") expected ""; got %q`, path)
	}

	// Setup
	sql := `CREATE TABLE x(a, b, c)`
	if err := c.Exec(sql); err != nil {
		t.Fatalf("c.Exec(%q) unexpected error: %v", sql, err)
	}
	sql = `INSERT INTO x VALUES(NULL, 42, ?)`
	if err := c.Exec(sql, "hello, world"); err != nil {
		t.Fatalf("c.Exec(%q) unexpected error: %v", sql, err)
	}
	if rowid := c.LastInsertId(); rowid != 1 {
		t.Fatalf("c.LastInsertId() expected 1; got %v", rowid)
	}

	// Query
	sql = `SELECT * FROM x ORDER BY rowid`
	if c.Exec(sql, 42) == nil {
		t.Fatalf("c.Exec(%q, 42) expected an error", sql)
	}
	s, err := c.Query(sql)
	if err != nil || s == nil {
		t.Fatalf("c.Query(%q) unexpected error: %v", sql, err)
	}
	defer t.close(s)

	// Statement information
	if s.Conn() != c {
		t.Fatalf("s.Conn() expected %v; got %v", c, s.Conn())
	}
	if !s.Valid() {
		t.Fatalf("s.Valid() expected true")
	}
	if !s.Busy() {
		t.Fatalf("s.Busy() expected true")
	}
	if !s.ReadOnly() {
		t.Fatalf("s.ReadOnly() expected true")
	}
	if s.String() != sql {
		t.Fatalf("s.String() expected %q; got %q", sql, s.String())
	}
	if s.NumParams() != 0 {
		t.Fatalf("s.NumParams() expected 0; got %d", s.NumParams())
	}
	if s.NumColumns() != 3 {
		t.Fatalf("s.NumColumns() expected 3; got %d", s.NumColumns())
	}
	if s.Params() != nil {
		t.Fatalf("s.Params() expected <nil>; got %v", s.Params())
	}

	// Column metadata
	cols := []string{"a", "b", "c"}
	if !reflect.DeepEqual(cols, s.Columns()) {
		t.Fatalf("s.Columns() expected %v; got %v", cols, s.Columns())
	}
	decls := []string{"", "", ""}
	if !reflect.DeepEqual(decls, s.DeclTypes()) {
		t.Fatalf("s.DeclTypes() expected %v; got %v", decls, s.DeclTypes())
	}
	dtypes := []byte{NULL, INTEGER, TEXT}
	if !reflect.DeepEqual(dtypes, s.DataTypes()) {
		t.Fatalf("s.DataTypes() expected %v; got %v", dtypes, s.DataTypes())
	}

	// Scanning into variables
	var _a interface{}
	var _b int
	var _c string
	if err := s.Scan(&_a, &_b, &_c); err != nil {
		t.Fatalf("s.Scan() unexpected error: %v", err)
	}
	if _a != nil {
		t.Fatalf("s.Scan(&_a, _, _) expected <nil>; got %v", _a)
	}
	if _b != 42 {
		t.Fatalf("s.Scan(_, &_b, _) expected 42; got %d", _b)
	}
	if _c != "hello, world" {
		t.Fatalf(`s.Scan(_, _, &_c) expected "hello, world"; got %q`, _c)
	}

	// Scanning into RowMap
	have := make(RowMap)
	want := RowMap{"a": nil, "b": int64(42), "c": "hello, world"}
	if err := s.Scan(have); err != nil {
		t.Fatalf("s.Scan(have) unexpected error: %v", err)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatalf("s.Scan(have) expected %v; got %v", want, have)
	}

	// Mixed scanning
	_a = "bad"
	have = make(RowMap)
	delete(want, "a")
	if err := s.Scan(&_a, have); err != nil {
		t.Fatalf("s.Scan(&_a, have) unexpected error: %v", err)
	}
	if _a != nil {
		t.Fatalf("s.Scan(&_a, _) expected <nil>; got %v", _a)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatalf("s.Scan(_, have) expected %v; got %v", want, have)
	}

	// End of rows
	if err := s.Next(); err != io.EOF {
		t.Fatalf("s.Next() expected EOF; got %v", err)
	}
	if s.Busy() {
		t.Fatalf("s.Busy() expected false")
	}
	if s.DataTypes() != nil {
		t.Fatalf("s.DataTypes() expected <nil>; got %v", s.DataTypes())
	}

	// Close
	t.close(s)
	if s.Conn() != c {
		t.Fatalf("s.Conn() expected %v; got %v", c, s.Conn())
	}
	if s.Valid() {
		t.Fatalf("s.Valid() expected false")
	}
}

func TestNull(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	c.Exec("CREATE TABLE t(text, blob)")

	var text string
	var blob []byte

	(*reflect.StringHeader)(unsafe.Pointer(&text)).Data = 0
	(*reflect.SliceHeader)(unsafe.Pointer(&blob)).Data = 0

	sql := "INSERT INTO t VALUES(?, ?)"
	c.Exec(sql, text, blob)

	(*reflect.StringHeader)(unsafe.Pointer(&text)).Data = 1
	(*reflect.SliceHeader)(unsafe.Pointer(&blob)).Data = 1

	c.Exec(sql, text, blob)

	s, _ := c.Query("SELECT * FROM t")
	defer t.close(s)

	want := []byte{TEXT, BLOB}
	for i := 1; i <= 2; i++ {
		if have := s.DataTypes(); !reflect.DeepEqual(want, have) {
			t.Errorf("Row %d expected %v; got %v", i, want, have)
		}
		s.Next()
	}
}

func TestTail(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	head := "CREATE TABLE x(y);"
	tail := " -- comment"
	sql := head + tail

	s, err := c.Prepare(head)
	if err != nil {
		t.Fatalf("c.Prepare(head) unexpected error: %v", err)
	}
	defer t.close(s)
	if want := ""; want != s.Tail {
		t.Fatalf("s.Tail expected %q; got %q", want, s.Tail)
	}

	s, err = c.Prepare(sql)
	if err != nil {
		t.Fatalf("c.Prepare(sql) unexpected error: %v", err)
	}
	defer t.close(s)
	if s.String() != head {
		t.Fatalf("s.String() expected %q; got %q", head, s.String())
	}
	if s.Tail != tail {
		t.Fatalf("s.Tail expected %q; got %q", tail, s.Tail)
	}

	have := (*reflect.StringHeader)(unsafe.Pointer(&s.Tail)).Data -
		(*reflect.StringHeader)(unsafe.Pointer(&sql)).Data
	if want := uintptr(len(head)); have != want {
		t.Fatalf("s.Tail isn't a pointer into sql")
	}

	s, err = c.Prepare(s.Tail)
	if err != nil {
		t.Fatalf("c.Prepare(s.Tail) unexpected error: %v", err)
	}
	if want := ""; want != s.Tail {
		t.Fatalf("s.Tail expected %q; got %q", want, s.Tail)
	}
	if s.Valid() {
		t.Fatalf("s.Valid() expected false")
	}
}

func TestParams(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	// Setup
	sql := `CREATE TABLE x(a, b, c)`
	if err := c.Exec(sql); err != nil {
		t.Fatalf("c.Exec(%q) unexpected error: %v", sql, err)
	}

	// Unnamed parameters
	sql = `INSERT INTO x VALUES(?, ?, ?)`
	s, err := c.Prepare(sql)
	if err != nil || s == nil {
		t.Fatalf("c.Prepare(%q) unexpected error: %v", sql, err)
	}
	defer t.close(s)

	// Parameter information
	if s.NumParams() != 3 {
		t.Fatalf("s.NumParams() expected 3; got %d", s.NumParams())
	}
	if s.Params() != nil {
		t.Fatalf("s.Params() expected <nil>; got %v", s.Params())
	}

	// Bad arguments
	if s.Exec() == nil {
		t.Fatalf("s.Exec() expected an error")
	}
	if s.Exec(1, 2, 3, 4) == nil {
		t.Fatalf("s.Exec(1, 2, 3, 4) expected an error")
	}
	if s.Exec(NamedArgs{}) == nil {
		t.Fatalf("s.Exec(NamedArgs{}) expected an error")
	}

	// Multiple inserts
	if err := s.Exec(1, 2, 3); err != nil {
		t.Fatalf("s.Exec(1, 2, 3) unexpected error: %v", err)
	}
	if err := s.Exec(1.1, 2.2, 3.3); err != nil {
		t.Fatalf("s.Exec(1, 2, 3) unexpected error: %v", err)
	}

	// Named parameters
	sql = `INSERT INTO x VALUES(:a, @B, $c)`
	s, err = c.Prepare(sql)
	if err != nil || s == nil {
		t.Fatalf("c.Prepare(%q) unexpected error: %v", sql, err)
	}
	defer t.close(s)

	// Parameter information
	if s.NumParams() != 3 {
		t.Fatalf("s.NumParams() expected 3; got %d", s.NumParams())
	}
	params := []string{":a", "@B", "$c"}
	if !reflect.DeepEqual(params, s.Params()) {
		t.Fatalf("s.Params() expected %v; got %v", params, s.Params())
	}

	// Multiple inserts
	if err := s.Exec("a", "b", "c"); err != nil {
		t.Fatalf(`s.Exec("x", "y", "z") unexpected error: %v`, err)
	}
	args := NamedArgs{
		":a": []byte("X"),
		"@B": []byte("Y"),
		"$C": []byte("*"),
	}
	if err := s.Query(args); err != io.EOF {
		t.Fatalf("s.Query(args) expected EOF; got %v", err)
	}

	// Select all rows
	sql = `SELECT rowid, * FROM x ORDER BY rowid`
	if s, err = c.Query(sql); err != nil {
		t.Fatalf("c.Query() unexpected error: %v", err)
	}
	defer t.close(s)

	// Verify
	table := []RowMap{
		{"rowid": int64(1), "a": int64(1), "b": int64(2), "c": int64(3)},
		{"rowid": int64(2), "a": 1.1, "b": 2.2, "c": 3.3},
		{"rowid": int64(3), "a": "a", "b": "b", "c": "c"},
		{"rowid": int64(4), "a": []byte("X"), "b": []byte("Y"), "c": nil},
	}
	have := make(RowMap)
	for i, want := range table {
		if err := s.Scan(have); err != nil {
			t.Fatalf("s.Scan(have) unexpected error: %v", err)
		}
		if !reflect.DeepEqual(want, have) {
			t.Fatalf("s.Scan(have) expected %v; got %v", want, have)
		}
		if i < len(table)-1 {
			if err := s.Next(); err != nil {
				t.Fatalf("s.Next(%d) unexpected error: %v", i, err)
			}
		}
	}
	if err := s.Next(); err != io.EOF {
		t.Fatalf("s.Next() expected EOF; got %v", err)
	}
}

func TestIO(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	// Setup
	c.Exec(`CREATE TABLE x(a)`)
	c.Exec(`INSERT INTO x VALUES(?)`, ZeroBlob(8))
	c.Exec(`INSERT INTO x VALUES(?)`, "hello, world")

	b, err := c.BlobIO("main", "x", "a", 1, true)
	if err != nil || b == nil {
		t.Fatalf("c.BlobIO() unexpected error: %v", err)
	}
	defer t.close(b)

	// Blob information
	if b.Row() != 1 {
		t.Fatalf("b.Row() expected 1; got %d", b.Row())
	}
	if b.Len() != 8 {
		t.Fatalf("b.Len() expected 8; got %d", b.Len())
	}

	// Write
	in := []byte("1234567")
	if n, err := b.Write(in); n != 7 || err != nil {
		t.Fatalf("b.Write(%q) expected 7, <nil>; got %d, %v", in, n, err)
	}
	in = []byte("89")
	if n, err := b.Write(in); n != 0 || err != ErrBlobFull {
		t.Fatalf("b.Write(%q) expected 0, ErrBlobFull; got %d, %v", in, n, err)
	}

	// Reopen
	if err := b.Reopen(2); err != nil {
		t.Fatalf("b.Reopen(2) unexpected error: %v", err)
	}
	if b.Row() != 2 {
		t.Fatalf("b.Row() expected 2; got %d", b.Row())
	}
	if b.Len() != 12 {
		t.Fatalf("b.Len() expected 12; got %d", b.Len())
	}

	// Read
	for i := 0; i < 2; i++ {
		out := make([]byte, 13)
		if n, err := b.Read(out); n != 12 || err != nil {
			t.Fatalf("b.Read(%d) expected 12, <nil>; got %d, %v", i, n, err)
		}
		have := string(out)
		if want := "hello, world\x00"; have != want {
			t.Fatalf("b.Read(%d) expected %q; got %q", i, have, want)
		}

		// Seek to start
		if p, err := b.Seek(0, 0); p != 0 || err != nil {
			t.Fatalf("b.Seek() expected 0, <nil>; got %d, %v", p, err)
		}
	}
	t.close(b)

	// Verify
	s, _ := c.Query("SELECT * FROM x ORDER BY rowid")
	defer t.close(s)

	var have string
	s.Scan(&have)
	if want := "1234567\x00"; have != want {
		t.Fatalf("Row 1 expected %q; got %q", want, have)
	}
	s.Next()
	s.Scan(&have)
	if want := "hello, world"; have != want {
		t.Fatalf("Row 2 expected %q; got %q", want, have)
	}
}

func TestBackup(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c1, c2 := t.open(":memory:"), t.open(":memory:")
	defer t.close(c1)
	defer t.close(c2)

	// Setup (c1)
	c1.Exec(`CREATE TABLE x(a)`)
	c1.Exec(`INSERT INTO x VALUES(?)`, "1234567\x00")
	c1.Exec(`INSERT INTO x VALUES(?)`, "hello, world")

	// Backup
	b, err := c1.Backup("main", c2, "main")
	if err != nil || b == nil {
		t.Fatalf("b.Backup() unexpected error: %v", err)
	}
	defer t.close(b)
	if err = b.Step(-1); err != io.EOF {
		t.Fatalf("b.Step(-1) expected EOF; got %v", err)
	}
	t.close(b)

	// Verify (c2)
	s, _ := c2.Query("SELECT * FROM x ORDER BY rowid")
	defer t.close(s)

	var have string
	s.Scan(&have)
	if want := "1234567\x00"; have != want {
		t.Fatalf("Row 1 expected %q; got %q", want, have)
	}
	s.Next()
	s.Scan(&have)
	if want := "hello, world"; have != want {
		t.Fatalf("Row 2 expected %q; got %q", want, have)
	}
}

func TestTx(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	c := t.open(":memory:")
	defer t.close(c)

	c.Exec(`CREATE TABLE x(a)`)

	// Begin
	if err := c.Begin(); err != nil {
		t.Fatalf("c.Begin() unexpected error: %v", err)
	}
	c.Exec(`INSERT INTO x VALUES(?)`, 1)
	c.Exec(`INSERT INTO x VALUES(?)`, 2)

	// Commit
	if err := c.Commit(); err != nil {
		t.Fatalf("c.Commit() unexpected error: %v", err)
	}

	// Begin
	if err := c.Begin(); err != nil {
		t.Fatalf("c.Begin() unexpected error: %v", err)
	}
	c.Exec(`INSERT INTO x VALUES(?)`, 3)
	c.Exec(`INSERT INTO x VALUES(?)`, 4)

	// Rollback
	if err := c.Rollback(); err != nil {
		t.Fatalf("c.Rollback() unexpected error: %v", err)
	}

	// Verify
	s, _ := c.Query("SELECT * FROM x ORDER BY rowid")
	defer t.close(s)

	var i int
	if s.Scan(&i); i != 1 {
		t.Fatalf("Row 1 expected 1; got %d", i)
	}
	s.Next()
	if s.Scan(&i); i != 2 {
		t.Fatalf("Row 2 expected 2; got %d", i)
	}
	if err := s.Next(); err != io.EOF {
		t.Fatalf("s.Next() expected EOF; got %v", err)
	}
}

func TestDriver(T *testing.T) {
	t := begin(T)
	defer t.skipRestIfFailed()

	// Open
	c, err := sql.Open("sqlite3", ":memory:")
	if err != nil || c == nil {
		t.Fatalf("sql.Open() unexpected error: %v", err)
	}
	defer t.close(c)

	// Setup
	sql := "CREATE TABLE x(a, b, c)"
	r, err := c.Exec(sql)
	if err != nil || r == nil {
		t.Fatalf("c.Exec(%q) unexpected error: %v", sql, err)
	}
	if id, err := r.LastInsertId(); id != 0 || err != nil {
		t.Fatalf("r.LastInsertId() expected 0, <nil>; got %d, %v", id, err)
	}
	if n, err := r.RowsAffected(); n != 0 || err != nil {
		t.Fatalf("r.RowsAffected() expected 0, <nil>; got %d, %v", n, err)
	}

	// Prepare
	sql = "INSERT INTO x VALUES(?, ?, ?)"
	s, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("c.Prepare(%q) unexpected error: %v", sql, err)
	}
	defer t.close(s)

	// Multiple inserts
	r, err = s.Exec(1, 2.2, "test")
	if err != nil {
		t.Fatalf("s.Exec(%q) unexpected error: %v", sql, err)
	}
	if id, err := r.LastInsertId(); id != 1 || err != nil {
		t.Fatalf("r.LastInsertId() expected 1, <nil>; got %d, %v", id, err)
	}
	if n, err := r.RowsAffected(); n != 1 || err != nil {
		t.Fatalf("r.RowsAffected() expected 1, <nil>; got %d, %v", n, err)
	}

	r, err = s.Exec(3, []byte{4}, nil)
	if err != nil {
		t.Fatalf("s.Exec(%q) unexpected error: %v", sql, err)
	}
	if id, err := r.LastInsertId(); id != 2 || err != nil {
		t.Fatalf("r.LastInsertId() expected 1, <nil>; got %d, %v", id, err)
	}
	if n, err := r.RowsAffected(); n != 1 || err != nil {
		t.Fatalf("r.RowsAffected() expected 1, <nil>; got %d, %v", n, err)
	}

	// Select all rows
	sql = `SELECT rowid, * FROM x ORDER BY rowid`
	rows, err := c.Query(sql)
	if err != nil || rows == nil {
		t.Fatalf("c.Query() unexpected error: %v", err)
	}
	defer t.close(rows)

	// Row information
	want := []string{"rowid", "a", "b", "c"}
	if have, err := rows.Columns(); !reflect.DeepEqual(want, have) {
		t.Fatalf("rows.Columns() expected %v, <nil>; got %v, %v", want, have, err)
	}

	// Verify
	table := [][]interface{}{
		{int64(1), int64(1), float64(2.2), []byte("test")},
		{int64(2), int64(3), []byte{4}, nil},
	}
	for i, want := range table {
		if !rows.Next() {
			t.Fatalf("rows.Next(%d) expected true", i)
		}
		have := make([]interface{}, 4)
		if err := rows.Scan(&have[0], &have[1], &have[2], &have[3]); err != nil {
			t.Fatalf("rows.Scan() unexpected error: %v", err)
		}
		if !reflect.DeepEqual(want, have) {
			t.Fatalf("rows.Scan() expected %v; got %v", want, have)
		}
	}
	if rows.Next() {
		t.Fatalf("rows.Next() expected false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() unexpected error: %v", err)
	}
}
