// Copyright 2013 The Go-SQLite Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !windows,!go1.1

package sqlite3

/*
#cgo pkg-config: sqlite3

// To avoid using pkg-config, comment out the line above and uncomment the one
// below. Use -L to specify the directory containing libsqlite3.so.
//#cgo LDFLAGS: -lsqlite3
*/
import "C"

import (
	"os"
	"os/exec"
	"syscall"
)

// shell executes the sqlite3 command with the specified arguments.
func shell(args []string) int {
	path, err := exec.LookPath("sqlite3")
	if err != nil {
		return 127 // sqlite3 not found
	}
	cmd := exec.Command(path)
	cmd.Args = args
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
		}
		return 127
	}
	return 0
}

// errstr is a replacement for sqlite3_errstr, which was added in SQLite 3.7.15
// and may not be available on most *nix systems as of 2013-02-11.
func errstr(rc C.int) string {
	msg := "unknown error"
	switch rc {
	case ABORT_ROLLBACK:
		msg = "abort due to ROLLBACK"
	default:
		rc &= 0xff
		if rc >= 0 && rc < C.int(len(errMsg)) && errMsg[rc] != "" {
			msg = errMsg[rc]
		}
	}
	return msg
}

// errMsg contains copies of the error messages returned by sqlite3ErrStr (from
// SQLite 3.7.17).
var errMsg = [...]string{
	/* SQLITE_OK          */ "not an error",
	/* SQLITE_ERROR       */ "SQL logic error or missing database",
	/* SQLITE_INTERNAL    */ "",
	/* SQLITE_PERM        */ "access permission denied",
	/* SQLITE_ABORT       */ "callback requested query abort",
	/* SQLITE_BUSY        */ "database is locked",
	/* SQLITE_LOCKED      */ "database table is locked",
	/* SQLITE_NOMEM       */ "out of memory",
	/* SQLITE_READONLY    */ "attempt to write a readonly database",
	/* SQLITE_INTERRUPT   */ "interrupted",
	/* SQLITE_IOERR       */ "disk I/O error",
	/* SQLITE_CORRUPT     */ "database disk image is malformed",
	/* SQLITE_NOTFOUND    */ "unknown operation",
	/* SQLITE_FULL        */ "database or disk is full",
	/* SQLITE_CANTOPEN    */ "unable to open database file",
	/* SQLITE_PROTOCOL    */ "locking protocol",
	/* SQLITE_EMPTY       */ "table contains no data",
	/* SQLITE_SCHEMA      */ "database schema has changed",
	/* SQLITE_TOOBIG      */ "string or blob too big",
	/* SQLITE_CONSTRAINT  */ "constraint failed",
	/* SQLITE_MISMATCH    */ "datatype mismatch",
	/* SQLITE_MISUSE      */ "library routine called out of sequence",
	/* SQLITE_NOLFS       */ "large file support is disabled",
	/* SQLITE_AUTH        */ "authorization denied",
	/* SQLITE_FORMAT      */ "auxiliary database format error",
	/* SQLITE_RANGE       */ "bind or column index out of range",
	/* SQLITE_NOTADB      */ "file is encrypted or is not a database",
}
