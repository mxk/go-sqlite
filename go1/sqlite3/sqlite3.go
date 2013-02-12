//
// Written by Maxim Khitrov (February 2013)
//

package sqlite3

/*
#include "sqlite3.h"

// Macro for setting and clearing SQLite callbacks.
#define SET_CALLBACK(name, db, conn, enable) \
	if (enable) {                            \
		sqlite3_##name(db, go_##name, conn); \
	} else {                                 \
		sqlite3_##name(db, 0, 0);            \
	}

// util.go exports.
int go_busy_handler(void*, int);
int go_commit_hook(void*);
void go_rollback_hook(void*);
void go_update_hook(void*, int, const char*, const char*, sqlite3_int64);

// Functions for setting and clearing SQLite callbacks.
static void set_busy_handler(sqlite3 *db, void *conn, int enable) {
	SET_CALLBACK(busy_handler, db, conn, enable)
}
static void set_commit_hook(sqlite3 *db, void *conn, int enable) {
	SET_CALLBACK(commit_hook, db, conn, enable)
}
static void set_rollback_hook(sqlite3 *db, void *conn, int enable) {
	SET_CALLBACK(rollback_hook, db, conn, enable)
}
static void set_update_hook(sqlite3 *db, void *conn, int enable) {
	SET_CALLBACK(update_hook, db, conn, enable)
}

// cgo doesn't handle '...' arguments for sqlite3_{config,mprintf}.
static int init_config(int op) {
	return sqlite3_config(op);
}
static int init_config_uri(int onoff) {
	return sqlite3_config(SQLITE_CONFIG_URI, onoff);
}
static void init_temp_dir(const char *path) {
	sqlite3_temp_directory = sqlite3_mprintf("%s", path);
}

// cgo doesn't handle pointer constants for sqlite3_bind_{text,blob}.
static int bind_text_trans(sqlite3_stmt *s, int i, const char *p, int n) {
	return sqlite3_bind_text(s, i, p, n, SQLITE_TRANSIENT);
}
static int bind_blob_trans(sqlite3_stmt *s, int i, const void *p, int n) {
	if (n > 0) {
		return sqlite3_bind_blob(s, i, p, n, SQLITE_TRANSIENT);
	}
	// For consistency between []byte(nil) and []byte("")
	return sqlite3_bind_zeroblob(s, i, 0);
}
static int bind_text_static(sqlite3_stmt *s, int i, const char *p, int n) {
	return sqlite3_bind_text(s, i, p, n, SQLITE_STATIC);
}
static int bind_blob_static(sqlite3_stmt *s, int i, const void *p, int n) {
	if (n > 0) {
		return sqlite3_bind_blob(s, i, p, n, SQLITE_STATIC);
	}
	// For consistency between RawBytes(nil) and RawBytes("")
	return sqlite3_bind_zeroblob(s, i, 0);
}
*/
import "C"

import (
	"io"
	"os"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

// initerr is used to indicate a fatal initialization error, which disables this
// package, but allows the rest of the program to continue running.
var initerr error

// threadsafe is the current threading mode setting. It is initialized to the
// value of SQLITE_THREADSAFE and changed to 2 (multi-thread) in init.
var threadsafe = int(C.sqlite3_threadsafe())

func init() {
	if threadsafe != 0 && threadsafe != 2 {
		// Use multi-thread mode, unless the library was compiled with
		// -DSQLITE_THREADSAFE=0. There is no point in using serialized mode,
		// since code in this package is not thread-safe anyway (e.g. when
		// accessing error information).
		if rc := C.init_config(C.SQLITE_CONFIG_MULTITHREAD); rc != OK {
			initerr = libErr(rc, nil)
			return
		}
		threadsafe = 2
	}

	// Enable URI handling by default (requires SQLite version 3.7.7+).
	C.init_config_uri(1)

	// Use the same temporary directory as Go.
	// [http://www.sqlite.org/c3ref/temp_directory.html]
	tmp := os.TempDir() + "\x00"
	C.init_temp_dir(cStr(tmp))

	// "For maximum portability, it is recommended that applications always
	// invoke sqlite3_initialize() directly prior to using any other SQLite
	// interface. Future releases of SQLite may require this."
	// [http://www.sqlite.org/c3ref/initialize.html]
	if rc := C.sqlite3_initialize(); rc != OK {
		initerr = libErr(rc, nil)
		return
	}

	// Register database/sql driver.
	register("sqlite3")
}

// Conn is a connection handle, which may have multiple databases attached to it
// by using the ATTACH SQL statement.
// [http://www.sqlite.org/c3ref/sqlite3.html]
type Conn struct {
	db *C.sqlite3

	// Callbacks executed by the exported go_* functions in util.go
	busy     BusyFunc
	commit   CommitFunc
	rollback RollbackFunc
	update   UpdateFunc
}

// Open creates a new connection to a SQLite database. The name can be 1) a path
// to a file, which is created if it does not exist, 2) a URI using the syntax
// described at http://www.sqlite.org/uri.html, 3) the string ":memory:", which
// creates a temporary in-memory database, or 4) an empty string, which creates
// a temporary on-disk database (deleted when closed) in the directory returned
// by os.TempDir().
// [http://www.sqlite.org/c3ref/open.html]
func Open(name string) (*Conn, error) {
	if initerr != nil {
		return nil, initerr
	}
	name += "\x00"

	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cStr(name), &db,
		C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE, nil)
	if rc != OK {
		err := libErr(rc, db)
		C.sqlite3_close(db)
		return nil, err
	}

	c := &Conn{db: db}
	C.sqlite3_extended_result_codes(db, 1)
	runtime.SetFinalizer(c, func(c *Conn) { c.Close() })
	return c, nil
}

// Close releases all resources associated with the connection. If any prepared
// statements, incremental I/O operations, or backup operations are still
// active, the connection becomes an unusable "zombie" and is closed after all
// remaining statements and operations are destroyed. A BUSY error code is
// returned if the connection is left in this "zombie" status, which may
// indicate a programming mistake where some previously allocated resource was
// not properly released.
// [http://www.sqlite.org/c3ref/close.html]
func (c *Conn) Close() error {
	if db := c.db; db != nil {
		*c = Conn{}
		runtime.SetFinalizer(c, nil)
		if rc := C.sqlite3_close(db); rc != OK {
			err := libErr(rc, db)
			if rc == BUSY {
				C.sqlite3_close_v2(db)
			}
			return err
		}
	}
	return nil
}

// Prepare compiles the first statement in sql. Any remaining text after the
// first statement is saved in Stmt.Tail.
// [http://www.sqlite.org/c3ref/prepare.html]
func (c *Conn) Prepare(sql string) (*Stmt, error) {
	if c.db == nil {
		return nil, ErrBadConn
	}
	return newStmt(c, sql)
}

// Exec is a convenience method for executing one or more statements in sql.
// Arguments may be specified either as a list of unnamed interface{} values or
// as a single NamedArgs map. In unnamed mode, each statement consumes the
// required number of values from args. For example:
//
// 	c.Exec("UPDATE x SET a=?; UPDATE x SET b=?", 1, 2) // is executed as:
// 	// UPDATE x SET a=1
// 	// UPDATE x SET b=2
//
// When NamedArgs is used, the entire map is passed to every statement in sql,
// and unreferenced names are ignored. The following example is identical to the
// one above:
//
// 	args := NamedArgs{"@A": 1, "@B": 2}
// 	c.Exec("UPDATE x SET a=@A; UPDATE x SET b=@B", args)
//
// Without any arguments, the statements are executed by a single call to
// sqlite3_exec, which should be faster, especially for long SQL scripts.
func (c *Conn) Exec(sql string, args ...interface{}) error {
	if c.db == nil {
		return ErrBadConn
	}

	// Fast path via sqlite3_exec, which doesn't support parameter binding
	if len(args) == 0 {
		sql += "\x00"
		return c.exec(cStr(sql))
	}

	// Slow path via Prepare -> Exec -> Close
	unnamed := namedArgs(args) == nil
	execNext := func() error {
		s, err := newStmt(c, sql)
		if err != nil {
			return err
		}
		defer s.Close()

		sql = s.Tail
		if s.stmt == nil {
			return nil // Comment or whitespace
		}
		var myArgs []interface{}
		if s.nVars > 0 {
			if myArgs = args; unnamed {
				if s.nVars < len(myArgs) {
					myArgs = myArgs[:s.nVars]
				}
				args = args[len(myArgs):]
			}
		}
		return s.exec(myArgs)
	}
	var err error
	for sql != "" && err == nil {
		err = execNext()
	}
	if unnamed && err == nil && len(args) != 0 {
		return pkgErr(MISUSE, "%d argument(s) left unconsumed", len(args))
	}
	return err
}

// Query is a convenience method for executing the first query in sql. It
// returns either a prepared statement ready for scanning or an error, which
// will be io.EOF if the query did not return any rows.
func (c *Conn) Query(sql string, args ...interface{}) (*Stmt, error) {
	if c.db == nil {
		return nil, ErrBadConn
	}
	s, err := newStmt(c, sql)
	if err == nil {
		if err = s.Query(args...); err == nil {
			return s, nil
		}
		s.Close()
	}
	return nil, err
}

// Begin starts a new deferred transaction. Use c.Exec("BEGIN...") to start an
// immediate or an exclusive transaction.
// [http://www.sqlite.org/lang_transaction.html]
func (c *Conn) Begin() error {
	if c.db == nil {
		return ErrBadConn
	}
	return c.exec(cStr("BEGIN\x00"))
}

// Commit saves all changes made within a transaction to the database.
func (c *Conn) Commit() error {
	if c.db == nil {
		return ErrBadConn
	}
	return c.exec(cStr("COMMIT\x00"))
}

// Rollback aborts the current transaction without saving any changes.
func (c *Conn) Rollback() error {
	if c.db == nil {
		return ErrBadConn
	}
	return c.exec(cStr("ROLLBACK\x00"))
}

// Interrupt causes any pending database operation to abort and return at its
// earliest opportunity. It is safe to call this method from a goroutine
// different from the one that is currently running the database operation, but
// it is not safe to call this method on a connection that might close before
// the call returns.
// [http://www.sqlite.org/c3ref/interrupt.html]
func (c *Conn) Interrupt() {
	if db := c.db; db != nil {
		C.sqlite3_interrupt(db)
	}
}

// AutoCommit returns true if the database connection is in auto-commit mode
// (i.e. outside of an explicit transaction started by BEGIN).
// [http://www.sqlite.org/c3ref/get_autocommit.html]
func (c *Conn) AutoCommit() bool {
	if c.db == nil {
		return false
	}
	return C.sqlite3_get_autocommit(c.db) != 0
}

// LastInsertId returns the ROWID of the most recent successful INSERT
// statement.
// [http://www.sqlite.org/c3ref/last_insert_rowid.html]
func (c *Conn) LastInsertId() int64 {
	if c.db == nil {
		return 0
	}
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// RowsAffected returns the number of rows that were changed, inserted, or
// deleted by the most recent statement. Auxiliary changes caused by triggers or
// foreign key actions are not included (see Conn.TotalRowsAffected).
// [http://www.sqlite.org/c3ref/changes.html]
func (c *Conn) RowsAffected() int {
	if c.db == nil {
		return 0
	}
	return int(C.sqlite3_changes(c.db))
}

// TotalRowsAffected returns the number of rows that were changed, inserted, or
// deleted by the most recent statement, including changes caused by triggers or
// foreign key actions.
// [http://www.sqlite.org/c3ref/total_changes.html]
func (c *Conn) TotalRowsAffected() int {
	if c.db == nil {
		return 0
	}
	return int(C.sqlite3_total_changes(c.db))
}

// Backup starts an online database backup of c.srcName into dst.dstName.
// Connections c and dst must be distinct. All existing contents of the
// destination database are overwritten.
//
// A read lock is acquired on the source database only while it is being read
// during a call to Backup.Step. The source connection may be used for other
// purposes between these calls. The destination connection must not be used for
// anything until the backup is closed.
// [http://www.sqlite.org/backup.html]
func (c *Conn) Backup(srcName string, dst *Conn, dstName string) (*Backup, error) {
	if c.db == nil || c == dst || dst == nil || dst.db == nil {
		return nil, ErrBadConn
	}
	return newBackup(c, srcName, dst, dstName)
}

// BlobIO opens a BLOB or TEXT value for incremental I/O, allowing the value to
// be treated as a file for reading and/or writing. The value is located as if
// by the following query:
//
// 	SELECT col FROM db.tbl WHERE rowid=row
//
// If rw is true, the value is opened with read-write access, otherwise it is
// read-only. It is not possible to open a column that is part of an index or
// primary key for writing. If foreign key constraints are enabled, it is not
// possible to open a column that is part of a child key for writing.
// [http://www.sqlite.org/c3ref/blob_open.html]
func (c *Conn) BlobIO(db, tbl, col string, row int64, rw bool) (*BlobIO, error) {
	if c.db == nil {
		return nil, ErrBadConn
	}
	return newBlobIO(c, db, tbl, col, row, rw)
}

// BusyTimeout enables the built-in busy handler, which retries the table
// locking operation for the specified duration before aborting. It returns the
// callback function that was previously registered with Conn.BusyFunc, if any.
// The busy handler is disabled if d is negative or zero.
// [http://www.sqlite.org/c3ref/busy_timeout.html]
func (c *Conn) BusyTimeout(d time.Duration) (prev BusyFunc) {
	if c.db != nil {
		// BUG(mxk): The SQL statement "PRAGMA busy_timeout = n" does not clear
		// the BusyFunc registered with the connection (if there is one). This
		// causes the next call to Conn.BusyTimeout or Conn.BusyFunc to return a
		// previous handler that wasn't actually being used. This doesn't affect
		// the operation of SQLite in any way. Use Conn.BusyTimeout instead of
		// the PRAGMA to avoid this problem if the return value is relevant.
		prev, c.busy = c.busy, nil
		C.sqlite3_busy_timeout(c.db, C.int(d/time.Millisecond))
	}
	return
}

// BusyFunc registers a function that is invoked by SQLite when it is unable to
// acquire a lock on a table. It returns the previous busy handler, if any. The
// function f should return true to make another lock acquisition attempt, or
// false to let the operation fail with BUSY or IOERR_BLOCKED error code.
// [http://www.sqlite.org/c3ref/busy_handler.html]
func (c *Conn) BusyFunc(f BusyFunc) (prev BusyFunc) {
	if c.db != nil {
		prev, c.busy = c.busy, f
		C.set_busy_handler(c.db, unsafe.Pointer(c), cBool(f != nil))
	}
	return
}

// CommitFunc registers a function that is invoked by SQLite before a
// transaction is committed. It returns the previous commit handler, if any. If
// the function f returns true, the transaction is rolled back instead, causing
// the rollback handler to be invoked, if one is registered.
// [http://www.sqlite.org/c3ref/commit_hook.html]
func (c *Conn) CommitFunc(f CommitFunc) (prev CommitFunc) {
	if c.db != nil {
		prev, c.commit = c.commit, f
		C.set_commit_hook(c.db, unsafe.Pointer(c), cBool(f != nil))
	}
	return
}

// RollbackFunc registers a function that is invoked by SQLite when a
// transaction is rolled back. It returns the previous rollback handler, if any.
// [http://www.sqlite.org/c3ref/commit_hook.html]
func (c *Conn) RollbackFunc(f RollbackFunc) (prev RollbackFunc) {
	if c.db != nil {
		prev, c.rollback = c.rollback, f
		C.set_rollback_hook(c.db, unsafe.Pointer(c), cBool(f != nil))
	}
	return
}

// UpdateFunc registers a function that is invoked by SQLite when a row is
// updated, inserted, or deleted. It returns the previous update handler, if
// any.
// [http://www.sqlite.org/c3ref/update_hook.html]
func (c *Conn) UpdateFunc(f UpdateFunc) (prev UpdateFunc) {
	if c.db != nil {
		prev, c.update = c.update, f
		C.set_update_hook(c.db, unsafe.Pointer(c), cBool(f != nil))
	}
	return
}

// Path returns the full file path of an attached database. An empty string is
// returned for temporary databases.
// [http://www.sqlite.org/c3ref/db_filename.html]
func (c *Conn) Path(db string) string {
	if c.db != nil {
		db += "\x00"
		if path := C.sqlite3_db_filename(c.db, cStr(db)); path != nil {
			return C.GoString(path)
		}
	}
	return ""
}

// Status returns the current and peak values of a connection performance
// counter, specified by one of the DBSTATUS constants. If reset is true, the
// peak value is reset back down to the current value after retrieval.
// [http://www.sqlite.org/c3ref/db_status.html]
func (c *Conn) Status(op int, reset bool) (cur, peak int, err error) {
	if c.db == nil {
		return 0, 0, ErrBadConn
	}
	var cCur, cPeak C.int
	rc := C.sqlite3_db_status(c.db, C.int(op), &cCur, &cPeak, cBool(reset))
	if rc != OK {
		return 0, 0, pkgErr(MISUSE, "invalid connection status op (%d)", op)
	}
	return int(cCur), int(cPeak), nil
}

// Limit changes a per-connection resource usage or performance limit, specified
// by one of the LIMIT constants, returning its previous value. If the new value
// is negative, the limit is left unchanged and its current value is returned.
// [http://www.sqlite.org/c3ref/limit.html]
func (c *Conn) Limit(id, value int) (prev int) {
	if c.db != nil {
		prev = int(C.sqlite3_limit(c.db, C.int(id), C.int(value)))
	}
	return
}

// exec calls sqlite3_exec on sql, which must be a null-terminated C string.
func (c *Conn) exec(sql *C.char) error {
	if rc := C.sqlite3_exec(c.db, sql, nil, nil, nil); rc != OK {
		return libErr(rc, c.db)
	}
	return nil
}

// Stmt is a prepared statement handle.
// [http://www.sqlite.org/c3ref/stmt.html]
type Stmt struct {
	Tail string // Uncompiled portion of the SQL string passed to Conn.Prepare

	conn *Conn
	stmt *C.sqlite3_stmt

	text    string // SQL text used to create this statement (minus the Tail)
	nVars   int    // Number of bound parameters (or maximum ?NNN value)
	nCols   int    // Number of columns in each row
	haveRow bool   // Flag indicating row availability

	varNames []string // Names of bound parameters (or unnamedVars)
	colNames []string // Names of columns in the result set
	colDecls []string // Column type declarations in upper case
	colTypes []byte   // Data type codes for all columns in the current row
}

// newStmt creates a new prepared statement.
func newStmt(c *Conn, sql string) (*Stmt, error) {
	sql += "\x00"

	var stmt *C.sqlite3_stmt
	var tail *C.char
	if rc := C.sqlite3_prepare_v2(c.db, cStr(sql), -1, &stmt, &tail); rc != OK {
		return nil, libErr(rc, c.db)
	}

	// stmt will be nil if sql contained only comments or whitespace. s.Tail may
	// be useful to the caller, so s is still returned without an error.
	s := &Stmt{conn: c, stmt: stmt}
	if stmt != nil {
		s.nVars = int(C.sqlite3_bind_parameter_count(stmt))
		s.nCols = int(C.sqlite3_column_count(stmt))
		if s.nCols > 0 {
			s.colTypes = make([]byte, s.nCols)
		}
		runtime.SetFinalizer(s, func(s *Stmt) { s.Close() })
	}
	if tail != nil {
		// tail is a pointer into sql, which is a regular Go string on the heap,
		// so an extra C.GoString allocation can be avoided.
		s.Tail = goStr(tail)
	}
	return s, nil
}

// Close releases all resources associated with the prepared statement. This
// method can be called at any point in the statement's life cycle.
// [http://www.sqlite.org/c3ref/finalize.html]
func (s *Stmt) Close() error {
	if stmt := s.stmt; stmt != nil {
		// Tail, conn, and text keep their current values
		s.stmt = nil
		s.nVars = 0
		s.nCols = 0
		s.haveRow = false
		s.varNames = nil
		s.colNames = nil
		s.colDecls = nil
		s.colTypes = nil
		runtime.SetFinalizer(s, nil)
		if rc := C.sqlite3_finalize(stmt); rc != OK {
			return libErr(rc, s.conn.db)
		}
	}
	return nil
}

// Conn returns the connection that that created this prepared statement.
func (s *Stmt) Conn() *Conn {
	return s.conn
}

// Valid returns true if the prepared statement can be executed by calling Exec
// or Query. A new prepared statement may not be valid if the SQL string
// contained nothing but comments or whitespace.
func (s *Stmt) Valid() bool {
	return s.stmt != nil
}

// Busy returns true if the prepared statement is in the middle of execution
// with a row available for scanning. It is not necessary to reset a busy
// statement before making another call to Exec or Query.
func (s *Stmt) Busy() bool {
	return s.haveRow
}

// ReadOnly returns true if the prepared statement makes no direct changes to
// the content of the database file.
// [http://www.sqlite.org/c3ref/stmt_readonly.html]
func (s *Stmt) ReadOnly() bool {
	return s.stmt == nil || C.sqlite3_stmt_readonly(s.stmt) != 0
}

// String implements fmt.Stringer by returning the SQL text that was used to
// create this prepared statement.
// [http://www.sqlite.org/c3ref/sql.html]
func (s *Stmt) String() string {
	if s.text == "" && s.stmt != nil {
		if text := C.sqlite3_sql(s.stmt); text != nil {
			s.text = C.GoString(text)
		}
	}
	return s.text
}

// NumParams returns the number of bound parameters in the prepared statement.
// This is also the number of arguments required for calling Exec or Query
// without a NamedArgs map.
// [http://www.sqlite.org/c3ref/bind_parameter_count.html]
func (s *Stmt) NumParams() int {
	return s.nVars
}

// NumColumns returns the number of columns produced by the prepared statement.
// [http://www.sqlite.org/c3ref/column_count.html]
func (s *Stmt) NumColumns() int {
	return s.nCols
}

// unnamedVars is assigned to Stmt.varNames if the prepared statement does not
// use named parameters. It just causes s.varNames == nil to evaluate to false.
var unnamedVars = []string{}

// Params returns the names of bound parameters in the prepared statement. Nil
// is returned if the statement does not use named parameters.
// [http://www.sqlite.org/c3ref/bind_parameter_name.html]
func (s *Stmt) Params() []string {
	if s.varNames == nil && s.nVars > 0 {
		var names []string
		for i := 0; i < s.nVars; i++ {
			name := C.sqlite3_bind_parameter_name(s.stmt, C.int(i+1))
			if name == nil {
				names = unnamedVars
				break
			}
			if names == nil {
				names = make([]string, s.nVars)
			}
			names[i] = C.GoString(name)
		}
		s.varNames = names
	}
	if len(s.varNames) == 0 {
		return nil // unnamedVars != nil
	}
	return s.varNames
}

// Columns returns the names of columns produced by the prepared statement.
// [http://www.sqlite.org/c3ref/column_name.html]
func (s *Stmt) Columns() []string {
	if s.colNames == nil && s.nCols > 0 {
		names := make([]string, s.nCols)
		for i := range names {
			name := C.sqlite3_column_name(s.stmt, C.int(i))
			if name != nil {
				names[i] = C.GoString(name)
			}
		}
		s.colNames = names
	}
	return s.colNames
}

// DeclTypes returns the type declarations of columns produced by the prepared
// statement. The type declarations are normalized to upper case.
// [http://www.sqlite.org/c3ref/column_decltype.html]
func (s *Stmt) DeclTypes() []string {
	if s.colDecls == nil && s.nCols > 0 {
		decls := make([]string, s.nCols)
		for i := range decls {
			decl := C.sqlite3_column_decltype(s.stmt, C.int(i))
			if decl != nil {
				decls[i] = strings.ToUpper(C.GoString(decl))
			}
		}
		s.colDecls = decls
	}
	return s.colDecls
}

// DataTypes returns the data type codes of columns in the current row. Possible
// data types are INTEGER, FLOAT, TEXT, BLOB, and NULL. These represent the
// actual storage classes used by SQLite to store each value. The returned slice
// should not be modified.
// [http://www.sqlite.org/c3ref/column_blob.html]
func (s *Stmt) DataTypes() []byte {
	if !s.haveRow {
		return nil
	}
	for i, typ := range s.colTypes {
		if typ == 0 {
			s.colTypes[i] = byte(C.sqlite3_column_type(s.stmt, C.int(i)))
		}
	}
	return s.colTypes
}

// Exec executes and resets the prepared statement. No rows are returned.
// [http://www.sqlite.org/c3ref/step.html]
func (s *Stmt) Exec(args ...interface{}) error {
	if s.stmt == nil {
		return ErrBadStmt
	}
	err := s.exec(args)
	if s.haveRow {
		s.Reset()
	}
	return err
}

// Query executes the prepared statement and makes the first returned row
// available for scanning. io.EOF is returned and the statement is reset if the
// query does not return any rows.
func (s *Stmt) Query(args ...interface{}) error {
	if s.stmt == nil {
		return ErrBadStmt
	}
	err := s.exec(args)
	if !s.haveRow && err == nil {
		return io.EOF
	}
	return err
}

// Scan retrieves data from the current row, storing successive column values
// into successive arguments. If the last argument is an instance of RowMap,
// then all remaining column/value pairs are assigned into the map. The same row
// may be scanned multiple times. Nil arguments are silently skipped.
// [http://www.sqlite.org/c3ref/column_blob.html]
func (s *Stmt) Scan(dst ...interface{}) error {
	if !s.haveRow {
		return io.EOF
	}
	n := len(dst)
	if n == 0 {
		return nil
	}
	if n > s.nCols {
		return pkgErr(MISUSE, "cannot assign %d value(s) from %d column(s)",
			n, s.nCols)
	}
	rowMap, _ := dst[n-1].(RowMap)
	if rowMap != nil {
		n--
	}
	for i, v := range dst[:n] {
		if v != nil {
			if err := s.scan(C.int(i), v); err != nil {
				return err
			}
		}
	}
	if rowMap != nil {
		var v interface{}
		for i, col := range s.Columns()[n:] {
			if err := s.scanDynamic(C.int(n+i), &v, false); err != nil {
				return err
			}
			rowMap[col] = v
		}
	}
	return nil
}

// Next makes the next row available for scanning. io.EOF is returned and the
// statement is reset if no more rows are available.
func (s *Stmt) Next() error {
	if s.haveRow {
		if err := s.step(); err != nil {
			return err
		}
		if s.haveRow {
			return nil
		}
	}
	return io.EOF
}

// Reset returns the prepared statement to its initial state, ready to be
// re-executed. This should be done when the remaining rows returned by a query
// are not needed, which releases some resources that would otherwise persist
// until the next call to Exec or Query.
// [http://www.sqlite.org/c3ref/reset.html]
func (s *Stmt) Reset() {
	if s.haveRow {
		s.haveRow = false
		C.sqlite3_reset(s.stmt)
		if s.nVars > 0 {
			C.sqlite3_clear_bindings(s.stmt)
		}
	}
}

// Status returns the current value of a statement performance counter,
// specified by one of the STMTSTATUS constants. If reset is true, the value is
// reset back down to 0 after retrieval.
// [http://www.sqlite.org/c3ref/stmt_status.html]
func (s *Stmt) Status(op int, reset bool) int {
	if s.stmt == nil {
		return 0
	}
	return int(C.sqlite3_stmt_status(s.stmt, C.int(op), cBool(reset)))
}

// exec resets the prepared statement, binds new parameter values, and executes
// the first step.
func (s *Stmt) exec(args []interface{}) (err error) {
	if s.haveRow {
		s.Reset()
	}
	if named := namedArgs(args); named != nil {
		err = s.bindNamed(named)
	} else {
		err = s.bindUnnamed(args)
	}
	if err != nil {
		if s.nVars > 0 {
			C.sqlite3_clear_bindings(s.stmt)
		}
		return
	}
	return s.step()
}

// bindNamed binds statement parameters using the name/value pairs in args.
func (s *Stmt) bindNamed(args NamedArgs) error {
	if s.nVars == 0 {
		return nil
	}
	names := s.Params()
	if names == nil {
		return pkgErr(MISUSE, "statement does not accept named arguments")
	}
	for i, name := range names {
		if err := s.bind(C.int(i+1), args[name], name); err != nil {
			return err
		}
	}
	return nil
}

// bindUnnamed binds statement parameters using successive values in args.
func (s *Stmt) bindUnnamed(args []interface{}) error {
	if len(args) != s.nVars {
		return pkgErr(MISUSE, "statement requires %d argument(s), %d given",
			s.nVars, len(args))
	}
	for i, v := range args {
		if err := s.bind(C.int(i+1), v, ""); err != nil {
			return err
		}
	}
	return nil
}

// bind binds statement parameter i (starting at 1) to the value v. The
// parameter name is only used for error reporting.
func (s *Stmt) bind(i C.int, v interface{}, name string) error {
	if v == nil {
		return nil // Unbound parameters are NULL by default
	}
	var rc C.int
	switch v := v.(type) {
	case int:
		rc = C.sqlite3_bind_int(s.stmt, i, C.int(v))
	case int64:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v))
	case float64:
		rc = C.sqlite3_bind_double(s.stmt, i, C.double(v))
	case bool:
		rc = C.sqlite3_bind_int(s.stmt, i, cBool(v))
	case string:
		rc = C.bind_text_trans(s.stmt, i, cStr(v), C.int(len(v)))
	case []byte:
		rc = C.bind_blob_trans(s.stmt, i, cBytes(v), C.int(len(v)))
	case time.Time:
		rc = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(v.Unix()))
	case RawString:
		rc = C.bind_text_static(s.stmt, i, cStr(string(v)), C.int(len(v)))
	case RawBytes:
		rc = C.bind_blob_static(s.stmt, i, cBytes(v), C.int(len(v)))
	case ZeroBlob:
		rc = C.sqlite3_bind_zeroblob(s.stmt, i, C.int(v))
	default:
		if name != "" {
			return pkgErr(MISUSE, "unsupported type for %s (%T)", name, v)
		}
		return pkgErr(MISUSE, "unsupported type at index %d (%T)", int(i-1), v)
	}
	if rc != OK {
		return libErr(rc, s.conn.db)
	}
	return nil
}

// step evaluates the next step in the statement's program, automatically
// resetting the statement if the result is anything other than SQLITE_ROW.
func (s *Stmt) step() error {
	s.haveRow = C.sqlite3_step(s.stmt) == ROW
	if s.haveRow {
		// Clear previous data types and reload new ones on demand
		for i := range s.colTypes {
			s.colTypes[i] = 0
		}
	} else {
		// If step returned DONE, reset returns OK. Otherwise, reset returns the
		// same error code as step (v2 interface).
		rc := C.sqlite3_reset(s.stmt)
		if s.nVars > 0 {
			C.sqlite3_clear_bindings(s.stmt)
		}
		if rc != OK {
			return libErr(rc, s.conn.db)
		}
	}
	return nil
}

// colType returns the data type code of column i in the current row (one of
// INTEGER, FLOAT, TEXT, BLOB, or NULL). The value becomes undefined after a
// type conversion, so this method must be called for column i to cache the
// original value before using any other sqlite3_column_* functions.
func (s *Stmt) colType(i C.int) (typ byte) {
	if typ = s.colTypes[i]; typ == 0 {
		typ = byte(C.sqlite3_column_type(s.stmt, i))
		s.colTypes[i] = typ
	}
	return
}

// scan scans the value of column i (starting at 0) into v.
func (s *Stmt) scan(i C.int, v interface{}) error {
	if typ := s.colType(i); typ == NULL {
		return s.scanZero(i, v)
	}
	switch v := v.(type) {
	case *interface{}:
		return s.scanDynamic(i, v, false)
	case *int:
		*v = int(C.sqlite3_column_int(s.stmt, i))
	case *int64:
		*v = int64(C.sqlite3_column_int64(s.stmt, i))
	case *float64:
		*v = float64(C.sqlite3_column_double(s.stmt, i))
	case *bool:
		*v = C.sqlite3_column_int64(s.stmt, i) != 0
	case *string:
		*v = text(s.stmt, i, true)
	case *[]byte:
		*v = blob(s.stmt, i, true)
	case *time.Time:
		*v = time.Unix(int64(C.sqlite3_column_int64(s.stmt, i)), 0)
	case *RawString:
		*v = RawString(text(s.stmt, i, false))
	case *RawBytes:
		*v = RawBytes(blob(s.stmt, i, false))
	case io.Writer:
		if _, err := v.Write(blob(s.stmt, i, false)); err != nil {
			return err
		}
	default:
		return pkgErr(MISUSE, "unscannable type for column %d (%T)", int(i), v)
	}
	// BUG(mxk): If a SQLite memory allocation fails while scanning column
	// values, the error is not reported until the next call to Stmt.Next or
	// Stmt.Close. This behavior may change in the future to check for and
	// return the error immediately from Stmt.Scan.
	return nil
}

// scanZero assigns the zero value to v when the associated column is NULL.
func (s *Stmt) scanZero(i C.int, v interface{}) error {
	switch v := v.(type) {
	case *interface{}:
		*v = nil
	case *int:
		*v = 0
	case *int64:
		*v = 0
	case *float64:
		*v = 0.0
	case *bool:
		*v = false
	case *string:
		*v = ""
	case *[]byte:
		*v = nil
	case *time.Time:
		*v = time.Time{}
	case *RawString:
		*v = ""
	case *RawBytes:
		*v = nil
	case io.Writer:
	default:
		return pkgErr(MISUSE, "unscannable type for column %d (%T)", int(i), v)
	}
	return nil
}

// scanDynamic scans the value of column i (starting at 0) into v, using the
// column's data type and declaration to select an appropriate representation.
// If driverValue is true, the range of possible representations is restricted
// to those allowed by driver.Value.
func (s *Stmt) scanDynamic(i C.int, v *interface{}, driverValue bool) error {
	switch typ := s.colType(i); typ {
	case INTEGER:
		n := int64(C.sqlite3_column_int64(s.stmt, i))
		*v = n
		if decl := s.DeclTypes()[i]; len(decl) >= 4 {
			switch decl[:4] {
			case "DATE", "TIME":
				*v = time.Unix(n, 0)
			case "BOOL":
				*v = n != 0
			}
		}
	case FLOAT:
		*v = float64(C.sqlite3_column_double(s.stmt, i))
	case TEXT:
		if driverValue {
			*v = blob(s.stmt, i, true)
		} else {
			*v = text(s.stmt, i, true)
		}
	case BLOB:
		*v = blob(s.stmt, i, true)
	case NULL:
		*v = nil
	default:
		*v = nil
		return pkgErr(ERROR, "unknown column type (%d)", typ)
	}
	return nil
}

// text returns the value of column i as a string. If copy is false, the string
// will point to memory allocated by SQLite.
func text(stmt *C.sqlite3_stmt, i C.int, copy bool) string {
	p := (*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt, i)))
	if n := C.sqlite3_column_bytes(stmt, i); n > 0 {
		if copy {
			return C.GoStringN(p, n)
		}
		return goStrN(p, n)
	}
	return ""
}

// blob returns the value of column i as a []byte. If copy is false, the []byte
// will point to memory allocated by SQLite.
func blob(stmt *C.sqlite3_stmt, i C.int, copy bool) []byte {
	if p := C.sqlite3_column_blob(stmt, i); p != nil {
		n := C.sqlite3_column_bytes(stmt, i)
		if copy {
			return C.GoBytes(p, n)
		}
		return goBytes(p, n)
	}
	return nil
}
