//go:build linux

package runtime

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

func TestActiveLinuxProcessGroupMembersExcludesZombies(t *testing.T) {
	root := t.TempDir()
	for pid, stat := range map[string]string{
		"101": "101 (live worker) S 1 77 77 0 0\n",
		"102": "102 (dead worker) Z 1 77 77 0 0\n",
		"103": "103 (other worker) S 1 88 88 0 0\n",
	} {
		directory := filepath.Join(root, pid)
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "stat"), []byte(stat), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := activeLinuxProcessGroupMembersAt(root, 77)
	if err != nil || !reflect.DeepEqual(got, []int{101}) {
		t.Fatalf("members=%v err=%v", got, err)
	}
}

func TestWaitForLinuxProcessGroupBoundsLiveAndESRCHRaces(t *testing.T) {
	tests := []struct {
		name string
		open func(int) (int, error)
	}{
		{name: "live", open: func(int) (int, error) { return syscall.Open("/dev/null", syscall.O_RDONLY, 0) }},
		{name: "all-esrch", open: func(int) (int, error) { return -1, syscall.ESRCH }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			remaining := []time.Duration{time.Second, 0}
			checks := 0
			closed := false
			terminated, err := waitForLinuxProcessGroup(77,
				func() time.Duration {
					value := remaining[min(checks, len(remaining)-1)]
					checks++
					return value
				},
				func(int) ([]int, error) { return []int{101}, nil },
				test.open,
				func([]int, time.Duration) error { closed = true; return syscall.EINTR },
			)
			if err != nil || terminated || checks != 2 {
				t.Fatalf("terminated=%t checks=%d err=%v", terminated, checks, err)
			}
			if test.name == "live" && !closed {
				t.Fatal("live pidfd was not waited")
			}
			if test.name == "all-esrch" && closed {
				t.Fatal("ESRCH pidfd was passed to wait")
			}
		})
	}
}

func TestWaitForLinuxProcessGroupFailsClosedOnScanError(t *testing.T) {
	want := errors.New("scan failed")
	terminated, err := waitForLinuxProcessGroup(77, func() time.Duration { return time.Second }, func(int) ([]int, error) {
		return nil, want
	}, nil, nil)
	if terminated || !errors.Is(err, want) {
		t.Fatalf("terminated=%t err=%v", terminated, err)
	}
}
