//go:build windows

package config

import "os"

// Native Windows has no Unix UID contract. Agent Symphony's supported Windows
// runtime is WSL, but keep configuration parsing cross-compilable.
func safeExecutableOwner(os.FileInfo) bool { return true }
