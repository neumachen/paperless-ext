//go:build integration

package integration

import "syscall"

// makeFIFO creates a named pipe, so the "special file" eligibility case is
// exercised against a real special file rather than described.
func makeFIFO(path string) error { return syscall.Mkfifo(path, 0o644) }
