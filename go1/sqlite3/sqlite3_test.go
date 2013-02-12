//
// Written by Maxim Khitrov (February 2013)
//

package sqlite3_test

import (
	"database/sql"
	"io"
	"reflect"
	"testing"

	. "code.google.com/p/go-sqlite/go1/sqlite3"
)

// minVersion is the minimum required SQLite version. The package will not build
// with anything less, so it's only used to check that VersionNum is working.
const minVersion = 3007014

// skip causes all remaining tests to be skipped when set to true.
var skip = false

// Test control functions.
func checkSkip(t *testing.T) {
	if skip {
		t.Fatalf("test skipped")
	}
}
func skipIfFailed(t *testing.T) {
	skip = t.Failed()
}

// Object control functions.
func openConn(t *testing.T, name string) *Conn {
	c, err := Open(name)
	if err != nil || c == nil {
		t.Fatalf("Open(%q) unexpected error: %v", name, err)
	}
	return c
}
func closeConn(t *testing.T, c *Conn) {
	if err := c.Close(); err != nil {
		t.Fatalf("c.Close() unexpected error: %v", err)
	}
}
func closeStmt(t *testing.T, s *Stmt) {
	if err := s.Close(); err != nil {
		t.Fatalf("s.Close() unexpected error: %v", err)
	}
}
func closeBackup(t *testing.T, b *Backup) {
	if err := b.Close(); err != nil {
		t.Fatalf("b.Close() unexpected error: %v", err)
	}
}
func closeBlobIO(t *testing.T, b *BlobIO) {
	if err := b.Close(); err != nil {
		t.Fatalf("b.Close() unexpected error: %v", err)
	}
}

func TestInit(t *testing.T) {
	defer skipIfFailed(t)

	// Library information
	if SingleThread() {
		t.Log("!!!WARNING!!! SQLite was built with -DSQLITE_THREADSAFE=0")
	}
	if v := VersionNum(); v < minVersion {
		t.Fatalf("VersionNum() expected >= %d; got %d", minVersion, v)
	}

	// Direct open/close
	closeConn(t, openConn(t, ":memory:"))

	// Driver open/close
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil || db == nil {
		t.Fatalf("sql.Open() unexpected error: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("db.Close() unexpected error: %v", err)
	}

	// Check of assumptions for Stmt.Params()
	unnamedVars := []string{}
	if unnamedVars == nil {
		t.Fatalf("unnamedVars == nil")
	}
}

func TestBasic(t *testing.T) {
	checkSkip(t)
	defer skipIfFailed(t)

	c := openConn(t, ":memory:")
	defer closeConn(t, c)

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
	sql = `SELECT * FROM x`
	if c.Exec(sql, 42) == nil {
		t.Fatalf("c.Exec(%q, 42) expected an error", sql)
	}
	s, err := c.Query(sql)
	if err != nil || s == nil {
		t.Fatalf("c.Query(%q) unexpected error: %v", sql, err)
	}
	defer closeStmt(t, s)

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
	closeStmt(t, s)
	if s.Conn() != c {
		t.Fatalf("s.Conn() expected %v; got %v", c, s.Conn())
	}
	if s.Valid() {
		t.Fatalf("s.Valid() expected false")
	}
}

func TestParams(t *testing.T) {
	checkSkip(t)
	defer skipIfFailed(t)

	c := openConn(t, ":memory:")
	defer closeConn(t, c)

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
	defer closeStmt(t, s)

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

	// Multiple executions
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
	defer closeStmt(t, s)

	// Parameter information
	if s.NumParams() != 3 {
		t.Fatalf("s.NumParams() expected 3; got %d", s.NumParams())
	}
	params := []string{":a", "@B", "$c"}
	if !reflect.DeepEqual(params, s.Params()) {
		t.Fatalf("s.Params() expected %v; got %v", params, s.Params())
	}

	// Multiple executions
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

	// Select everything
	sql = `SELECT rowid, * FROM x`
	if s, err = c.Query(sql); err != nil {
		t.Fatalf("s.Query() unexpected error: %v", err)
	}
	defer closeStmt(t, s)

	// Verify
	table := []RowMap{
		RowMap{"rowid": int64(1), "a": int64(1), "b": int64(2), "c": int64(3)},
		RowMap{"rowid": int64(2), "a": 1.1, "b": 2.2, "c": 3.3},
		RowMap{"rowid": int64(3), "a": "a", "b": "b", "c": "c"},
		RowMap{"rowid": int64(4), "a": []byte("X"), "b": []byte("Y"), "c": nil},
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

func TestIO(t *testing.T) {
	checkSkip(t)
	defer skipIfFailed(t)

	c := openConn(t, ":memory:")
	defer closeConn(t, c)

	// Setup
	c.Exec(`CREATE TABLE x(a)`)
	c.Exec(`INSERT INTO x VALUES(?)`, ZeroBlob(8))
	c.Exec(`INSERT INTO x VALUES(?)`, "hello, world")

	b, err := c.BlobIO("main", "x", "a", 1, true)
	if err != nil || b == nil {
		t.Fatalf("c.BlobIO() unexpected error: %v", err)
	}
	defer closeBlobIO(t, b)

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
	closeBlobIO(t, b)

	// Verify
	s, _ := c.Query("SELECT * FROM x ORDER BY rowid")
	defer closeStmt(t, s)

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

func TestBackup(t *testing.T) {
	checkSkip(t)
	defer skipIfFailed(t)

	c1, c2 := openConn(t, ":memory:"), openConn(t, ":memory:")
	defer closeConn(t, c1)
	defer closeConn(t, c2)

	// Setup (c1)
	c1.Exec(`CREATE TABLE x(a)`)
	c1.Exec(`INSERT INTO x VALUES(?)`, "1234567\x00")
	c1.Exec(`INSERT INTO x VALUES(?)`, "hello, world")

	// Backup
	b, err := c1.Backup("main", c2, "main")
	if err != nil || b == nil {
		t.Fatalf("b.Backup() unexpected error: %v", err)
	}
	defer closeBackup(t, b)
	if err = b.Step(-1); err != io.EOF {
		t.Fatalf("b.Step(-1) expected EOF; got %v", err)
	}
	closeBackup(t, b)

	// Verify (c2)
	s, _ := c2.Query("SELECT * FROM x ORDER BY rowid")
	defer closeStmt(t, s)

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
