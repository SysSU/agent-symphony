package runtime

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

// Replace identity artifacts after certificate validation, at the first tmux I/O.
type replaceCertifiedStopRunner struct {
	Runner
	replace func()
}

func (r *replaceCertifiedStopRunner) Run(ctx context.Context, command Command) (Result, error) {
	if r.replace != nil {
		replace := r.replace
		r.replace = nil
		replace()
	}
	return r.Runner.Run(ctx, command)
}

func TestCertifiedStopRetainsOriginalIdentity(t *testing.T) {
	for _, replacement := range []string{"none", "pane and broker", "broker"} {
		t.Run(replacement, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := ReadImplementationBinding(manifest)
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := ReadTerminalBrokerBinding(TerminalBrokerPath(manifest))
			if err != nil {
				t.Fatal(err)
			}
			write := func(path string, value any) {
				t.Helper()
				body, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			r.Runner = &replaceCertifiedStopRunner{Runner: fake, replace: func() {
				fake.sessions[manifest.Session].dead = true
				if replacement == "pane and broker" {
					replaced := binding
					replaced.PaneID = "%99"
					fake.sessions[manifest.Session].paneID = replaced.PaneID
					write(ImplementationBindingPath(manifest, manifest.LaunchID), replaced)
				}
				replacedTerminal := terminal
				if replacement != "none" {
					replacedTerminal.InnerPID, replacedTerminal.InnerPGID, replacedTerminal.SocketIno = 702, 702, 2
					write(TerminalBrokerPath(manifest), replacedTerminal)
				}
				// A replacement's death certificate must not stand in for the original's.
				write(terminalBrokerDeadPath(TerminalBrokerPath(manifest)), replacedTerminal)
			}}
			err = r.StopCertifiedImplementation(t.Context(), manifest, binding, terminal)
			if replacement == "none" {
				if err != nil || fake.sessions[manifest.Session] != nil {
					t.Fatalf("original certified Stop did not finish: %v", err)
				}
				if err := r.StopCertifiedImplementation(t.Context(), manifest, binding, terminal); err != nil {
					t.Fatalf("certified Stop could not settle after cleanup: %v", err)
				}
				return
			}
			if fake.sessions[manifest.Session] == nil {
				t.Fatal("certified Stop killed a pane using replacement evidence")
			}
			if err == nil {
				t.Fatal("certified Stop completed without original worker death proof")
			}
		})
	}
}
