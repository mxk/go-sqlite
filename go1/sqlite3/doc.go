//
// Written by Maxim Khitrov (February 2013)
//

/*
Package sqlite3 provides an interface to SQLite version 3 databases.

Database connections are created either directly via this package or with the
"sqlite3" database/sql driver. The driver is recommended when your application
has to support multiple database engines. The direct interface exposes
SQLite-specific features, such as incremental I/O and online backups, and
provides better performance.

Installation

The package uses cgo to call SQLite library functions. Your system must have gcc
installed to build this package. Windows users should install mingw-w64
(http://mingw-w64.sourceforge.net/), TDM64-GCC (http://tdm-gcc.tdragon.net/), or
another MinGW distribution, and make sure that gcc.exe is available from your
PATH.

On Windows, the SQLite amalgamation source is compiled into the package, which
eliminates external dependencies. To use a different version of the library,
download and extract the amalgamation source from
http://www.sqlite.org/download.html, then copy sqlite3.h and sqlite3.c to the
package directory, renaming sqlite.c to sqlite3_windows.c.

On *nix, SQLite has to be used as a shared library. Eventually, it may be
possible to compile the amalgamation source into the package, as done on Windows
(see Go issue 4069). For now, pkg-config is used to locate and link with the
shared library. You can modify the #cgo lines in sqlite3_unix.go to change this
behavior.

The minimum version of the shared library that will work with this package is
3.7.14 (released 2012-09-03) due to the use of sqlite3_close_v2() interface.

Concurrency

A single connection and all related objects (prepared statements, backup
operations, etc.) may NOT be used concurrently from multiple goroutines without
external locking. All methods in this package, with the exception of
Conn.Interrupt, assume single-threaded operation. Depending on how SQLite was
compiled, it should be safe to use separate database connections concurrently,
even if they are accessing the same database file. For example:

	// ERROR (without any extra synchronization)
	c, _ := sqlite3.Open("./sqlite.db")
	go use(c)
	go use(c)

	// OK
	c1, _ := sqlite3.Open("./sqlite.db")
	c2, _ := sqlite3.Open("./sqlite.db")
	go use(c1)
	go use(c2)

If the SQLite library was compiled with -DSQLITE_THREADSAFE=0, then all mutex
code was omitted, and this package is unsafe for concurrent access even to
separate database connections. Use SingleThread() to determine if this is the
case. By default, SQLite is compiled with SQLITE_THREADSAFE=1, which enables
serialized threading mode. This package switches it to 2 (multi-thread) during
initialization for slightly better performance. See
http://www.sqlite.org/threadsafe.html for additional information.

Maps

NamedArgs and RowMap types are provided for using maps as statement arguments
and for query output, respectively. Here is a short usage example with the
error-handling code omitted for brevity:

	c, _ := sqlite3.Open(":memory:")
	c.Exec("CREATE TABLE x(a, b, c)")

	args := sqlite3.NamedArgs{"@a": 1, "@b": "demo"}
	c.Exec("INSERT INTO x VALUES(@a, @b, @c)", args) // @c will be NULL

	sql := "SELECT rowid, * FROM x"
	row := make(sqlite3.RowMap)
	for s, err := c.Query(sql); err == nil; err = s.Next() {
		var rowid int64
		s.Scan(&rowid, row) // Assign column 0 to rowid, the rest to row
		fmt.Println(rowid, row)
	}

Data Types

See http://www.sqlite.org/datatype3.html for documentation of the SQLite version
3 data type system. See http://www.sqlite.org/c3ref/column_blob.html for details
of how column values are retrieved from the results of a query.

The following data types are supported as arguments to prepared statements (and
may be used in NamedArgs):

	int
	int64
	float64
	bool      -- Bound as an int: false -> 0, true -> 1.
	string    -- Bound as a text value. SQLite makes an internal copy.
	[]byte    -- Bound as a BLOB value. SQLite makes an internal copy.
	time.Time -- Bound as an int64 after conversion via Unix().
	RawString -- Bound as a text value referencing Go's copy of the string. The
	             string must remain valid for the duration of the query.
	RawBytes  -- Bound as a BLOB value referencing Go's copy of the array. The
	             array must remain valid and unmodified for the duration of the
	             query.
	ZeroBlob  -- Allocates a zero-filled BLOB of the specified length
	             (e.g. ZeroBlob(4096) allocates 4KB).

The following static data types are supported for retrieving column values:

	*int
	*int64
	*float64
	*bool      -- Retrieved as an int64: 0 -> false, else -> true.
	*string    -- Retrieved as a text value and copied into Go-managed memory.
	*[]byte    -- Retrieved as a BLOB value and copied into Go-managed memory.
	*time.Time -- Retrieved as an int64 and converted via time.Unix(). TEXT
	              values are not supported, but see SQLite's date and time SQL
	              functions, which can perform the required conversion.
	*RawString -- Retrieved as a text value and returned as a string pointing
	              into SQLite's memory. The value remains valid as long as no
	              other Stmt methods are called.
	*RawBytes  -- Retrieved as a BLOB value and returned as a []byte pointing
	              into SQLite's memory. The value remains valid as long as no
	              other Stmt methods are called and must not be modified
	              (re-slicing is ok).
	io.Writer  -- Retrieved as a BLOB value and written out directly from
	              SQLite's memory into the writer.

The following rules are used for assigning column values to *interface{} and
RowMap arguments (dynamic typing). The SQLite's storage class and column
declaration are used to select the best Go representation:

	INTEGER -- Retrieved as an int64. If the column type declaration begins with
	           "DATE" or "TIME", convert via time.Unix(). If the declaration
	           begins with "BOOL", return a bool: 0 -> false, else -> true.
	FLOAT   -- Returned as a float64.
	TEXT    -- Returned as a string copy.
	BLOB    -- Returned as a []byte copy.
	NULL    -- Returned as nil.

Database Names

Methods that require a database name as one of the arguments (e.g. Conn.Path)
expect the symbolic name by which the database is known to the connection, not a
path to a file. Valid database names are "main", "temp", or a name specified
after the AS keyword in an ATTACH statement.

Callbacks

SQLite allows the user to install callback functions that are executed for
various internal events (e.g. busy handler and commit/rollback hooks). This
package defines the function types for these callbacks, which can be installed
for each Conn object. There are three important things to remember when using
these callbacks:

1. The callbacks are executed while SQLite is in the middle of a C function (Go
-> C -> Go). They are locked to the current thread, as though
runtime.LockOSThread() was called, and the Go runtime may have spawned
additional threads for running other goroutines.

2. The callbacks are not reentrant, meaning that they must not do anything that
will modify the database connection that invoked the callback. This includes
running/preparing any other SQL statements. The safest bet is to avoid all
interactions with Conn, Stmt, and other package objects within these callbacks.

3. Only one callback of each type can be installed for each connection. In
particular, Conn.BusyTimeout and Conn.BusyFunc are mutually exclusive. Setting
one clears the other. The former is a built-in busy handler that retries the
locking operation for the specified amount of time. It should be preferred over
BusyFunc when no additional logic is needed, since it avoids the transition
overhead between C and Go.
*/
package sqlite3
