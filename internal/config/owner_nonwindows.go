//go:build !windows

package config

import (
	"os"
	"syscall"
)

func safeExecutableOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return !ok || stat.Uid == 0 || int(stat.Uid) == os.Geteuid()
}
