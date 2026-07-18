//go:build !unix

package store

import (
	"errors"
	"os"
)

func acquireJournalLock(string) (*os.File, error) {
	return nil, errors.New("journal locking is unsupported on this platform")
}

func releaseJournalLock(*os.File) error {
	return nil
}
