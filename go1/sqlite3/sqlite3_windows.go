//
// Written by Maxim Khitrov (February 2013)
//

package sqlite3

/*
#include "sqlite3.h"

#cgo CFLAGS: -fno-stack-check -fno-stack-protector -mno-stack-arg-probe
*/
import "C"

// errstr uses the native implementation of sqlite3_errstr on Windows.
func errstr(rc C.int) string {
	return C.GoString(C.sqlite3_errstr(rc))
}
