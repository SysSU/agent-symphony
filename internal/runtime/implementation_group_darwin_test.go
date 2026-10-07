//go:build darwin

package runtime

import (
	"errors"
	"syscall"
	"testing"
)

func TestDarwinProcessGroupPermissionProbeUsesExactInventory(t *testing.T) {
	checked := false
	terminated, err := waitForDarwinProcessGroup(20, func(int) error { return syscall.EPERM }, func(pgid int) (bool, error) {
		checked = true
		if pgid != 20 {
			t.Fatalf("inventory pgid = %d, want 20", pgid)
		}
		return false, nil
	})
	if err != nil || !terminated || !checked {
		t.Fatalf("EPERM fallback terminated=%t checked=%t err=%v", terminated, checked, err)
	}
	denied := errors.New("inventory unavailable")
	if terminated, err = waitForDarwinProcessGroup(20, func(int) error { return denied }, func(int) (bool, error) {
		t.Fatal("inventory ran after an unknown probe failure")
		return false, nil
	}); terminated || !errors.Is(err, denied) {
		t.Fatalf("unknown probe terminated=%t err=%v", terminated, err)
	}
}

func TestParseDarwinProcessGroupIgnoresZombies(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want bool
	}{
		{name: "active member", body: " 10 20 S+\n 21 21 Z\n", want: true},
		{name: "zombie only", body: " 20 20 Z+\n", want: false},
		{name: "different group", body: " 10 30 R\n", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseDarwinProcessGroup([]byte(test.body), 20)
			if err != nil || got != test.want {
				t.Fatalf("active=%t want=%t err=%v", got, test.want, err)
			}
		})
	}
	if _, err := parseDarwinProcessGroup([]byte("invalid\n"), 20); err == nil {
		t.Fatal("malformed process inventory was accepted")
	}
}
