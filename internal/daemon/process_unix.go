//go:build darwin || linux

package daemon

import (
	"log/slog"
	"os/exec"
	"syscall"
	"time"
)

func setMonitorProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func monitorProcessGroup(cmd *exec.Cmd) (int, error) {
	if cmd == nil || cmd.Process == nil {
		return 0, syscall.ESRCH
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return 0, err
	}

	return pgid, nil
}

func terminateMonitorProcessGroup(pgid int, logger *slog.Logger) {
	if pgid <= 0 {
		return
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
		if err == syscall.ESRCH {
			return
		}
		logger.Warn("monitor process group sigterm failed", "pgid", pgid, "error", err)

		return
	}
	time.Sleep(2 * time.Second)
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		logger.Warn("monitor process group sigkill failed", "pgid", pgid, "error", err)
	}
}
