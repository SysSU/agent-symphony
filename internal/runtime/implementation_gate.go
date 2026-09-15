package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// RunImplementationGate is the initial tmux pane process. It remains parked
// until the exact launch is durably bound and released, then replaces itself
// with the worker command so the pane PID cannot silently change.
func RunImplementationGate(ctx context.Context, manifest Manifest, tmux string, command []string) error {
	if !ValidManifestVersion(manifest) || manifest.Version != boundManifestVersion || manifest.LaunchID == "" || tmux == "" || len(command) == 0 || command[0] == "" {
		return errors.New("implementation gate identity is incomplete")
	}
	channel := ImplementationGateChannel(manifest.LaunchID)
	if err := exec.CommandContext(ctx, tmux, "wait-for", "-L", channel).Run(); err != nil {
		return fmt.Errorf("wait for implementation launch: %w", err)
	}
	binding, err := ReadImplementationBinding(manifest)
	if err != nil || binding.PanePID != os.Getpid() || binding.PaneID != os.Getenv("TMUX_PANE") || !implementationPermitMatches(manifest, binding) || !implementationReleaseMatches(manifest, binding) {
		return errors.Join(err, errors.New("implementation gate has no exact durable release"))
	}
	if err := WriteImplementationGateEntered(manifest, binding); err != nil {
		return err
	}
	if err := exec.CommandContext(ctx, tmux, "wait-for", "-U", channel).Run(); err != nil {
		return fmt.Errorf("unlock implementation launch: %w", err)
	}
	path, err := exec.LookPath(command[0])
	if err != nil {
		return err
	}
	return syscall.Exec(path, command, os.Environ())
}
