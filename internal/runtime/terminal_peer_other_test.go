//go:build !darwin && !linux

package runtime

import "testing"

func TestTerminalPeerUnsupportedFailsClosed(t *testing.T) {
	if _, _, err := terminalSocketIdentity(nil); err == nil {
		t.Fatal("unsupported socket identity was accepted")
	}
	if err := verifyTerminalPeerPID(nil, 2); err == nil {
		t.Fatal("unsupported peer PID was accepted")
	}
}
