//go:build linux

package runtime

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestTerminalBrokerOuterDeadBeforeReaping(t *testing.T) {
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	binding := TerminalBrokerBinding{Version: terminalBrokerVersion, OuterPID: child.Process.Pid, InnerPID: 2, InnerPGID: 2, SocketPath: "/unused.sock", SocketDev: 1, SocketIno: 1, Secret: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	if dead, err := TerminalBrokerOuterDead(binding); err != nil || dead {
		t.Fatalf("live outer process: dead=%t err=%v", dead, err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		dead, err := TerminalBrokerOuterDead(binding)
		if err != nil {
			t.Fatal(err)
		}
		if dead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminated outer process was not proved dead before reaping")
		}
		time.Sleep(time.Millisecond)
	}
	if err := syscall.Kill(child.Process.Pid, 0); err != nil {
		t.Fatalf("fixture no longer retains an unreaped process: %v", err)
	}
}

func TestPollLinuxPIDFDAcceptsDescriptorAboveSelectCapacity(t *testing.T) {
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	pidfd, err := openLinuxPIDFD(child.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(pidfd)
	selectCapacity := len((syscall.FdSet{}).Bits) * 64
	highFD, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(pidfd), uintptr(syscall.F_DUPFD_CLOEXEC), uintptr(selectCapacity))
	if errno != 0 {
		t.Skipf("cannot allocate descriptor above select capacity: %v", errno)
	}
	defer syscall.Close(int(highFD))
	if int(highFD) < selectCapacity {
		t.Fatalf("descriptor=%d select capacity=%d", highFD, selectCapacity)
	}
	if ready, err := pollLinuxPIDFDs([]int{int(highFD)}, 0); err != nil || ready != 0 {
		t.Fatalf("live process: ready=%d err=%v", ready, err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		ready, err := pollLinuxPIDFDs([]int{int(highFD)}, 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if ready == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminated process did not make high pidfd readable")
		}
	}
	if err := child.Wait(); err == nil {
		t.Fatal("killed child unexpectedly exited successfully")
	}
	waited = true
}
