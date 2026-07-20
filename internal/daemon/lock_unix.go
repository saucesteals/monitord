//go:build darwin || linux

package daemon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/saucesteals/monitord/internal/config"
)

type daemonLock struct {
	file *os.File
}

func acquireDaemonLock(paths config.Paths) (*daemonLock, error) {
	if err := os.MkdirAll(paths.StateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	path := filepath.Join(paths.StateDir, "monitord.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("monitord daemon already running for %s", paths.Root)
		}

		return nil, fmt.Errorf("acquire daemon lock: %w", err)
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("truncate daemon lock: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("seek daemon lock: %w", err)
	}
	if _, err := fmt.Fprintf(file, "%d\n", os.Getpid()); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("write daemon lock: %w", err)
	}

	return &daemonLock{file: file}, nil
}

func (l *daemonLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	if err != nil {
		return fmt.Errorf("release daemon lock: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close daemon lock: %w", closeErr)
	}

	return nil
}
