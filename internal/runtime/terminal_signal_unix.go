//go:build darwin || linux

package runtime

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

func execCommand(name string, args ...string) *exec.Cmd { return exec.Command(name, args...) }

func terminalNotifyResize(channel chan<- os.Signal) { signal.Notify(channel, syscall.SIGWINCH) }
func terminalStopResize(channel chan<- os.Signal)   { signal.Stop(channel) }
