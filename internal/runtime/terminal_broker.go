package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

const (
	terminalBrokerVersion    = 1
	terminalReplayLimit      = 4 << 20
	terminalClientQueueLimit = 4 << 20
	terminalFrameLimit       = 64 << 10
	terminalOutputDrainWait  = 2 * time.Second
	terminalBrokerStopWait   = 45 * time.Second

	terminalFrameAuth   byte = 1
	terminalFrameStatus byte = 2
	terminalFrameStart  byte = 3
	terminalFrameOutput byte = 4
	terminalFrameInput  byte = 5
	terminalFrameResize byte = 6
)

// A silent member keeps the process-group identity allocated after the
// foreground command exits, until the broker can signal that exact group.
// The foreground worker is also the broker's unreaped direct child, so its
// zombie pins the PGID if it kills the whole group itself.
const terminalBrokerWrapper = `set +m
IFS= read -r ready <&3 || exit 125
[ "$ready" = go ] || exit 125
(trap '' HUP INT TERM; exec 0<&4 1>/dev/null 2>/dev/null 3>&- 4<&-; IFS= read -r _) &
exec "$@" 3>&- 4>&-`

func stopUnverifiedTerminalBrokerChild(inner *exec.Cmd, gate, hold *os.File) {
	_ = inner.Process.Kill()
	_ = gate.Close()
	_ = hold.Close()
	_ = inner.Wait()
}

// TerminalBrokerBinding is the immutable, owner-persisted identity needed to
// reach one broker. Secret is intentionally excluded from public projections.
type TerminalBrokerBinding struct {
	Version    int    `json:"version"`
	OuterPID   int    `json:"outer_pid"`
	InnerPID   int    `json:"inner_pid"`
	InnerPGID  int    `json:"inner_pgid"`
	SocketPath string `json:"socket_path"`
	SocketDev  uint64 `json:"socket_dev"`
	SocketIno  uint64 `json:"socket_ino"`
	Secret     string `json:"secret"`
}

type terminalBrokerRequest struct {
	Version int    `json:"version"`
	Secret  string `json:"secret"`
	Action  string `json:"action"`
}

type terminalBrokerStatus struct {
	OK       bool   `json:"ok"`
	Overflow bool   `json:"overflow,omitempty"`
	Error    string `json:"error,omitempty"`
}

type terminalBrokerResize struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

func TerminalBrokerPath(manifest Manifest) string {
	return ImplementationBindingPath(manifest, manifest.LaunchID) + ".terminal"
}

func TerminalBrokerSocketPath(recordPath, socketDir string) string {
	want := sha256.Sum256([]byte(recordPath))
	return filepath.Join(socketDir, "t-"+hex.EncodeToString(want[:16])+".sock")
}

func ValidTerminalBrokerAt(recordPath, socketDir string, binding TerminalBrokerBinding) bool {
	return filepath.IsAbs(recordPath) && filepath.Clean(recordPath) == recordPath && filepath.IsAbs(socketDir) && filepath.Clean(socketDir) == socketDir && ValidTerminalBrokerBinding(binding) && binding.SocketPath == TerminalBrokerSocketPath(recordPath, socketDir)
}

func ValidImplementationTerminalBinding(manifest Manifest, binding TerminalBrokerBinding) bool {
	return ValidTerminalBrokerAt(TerminalBrokerPath(manifest), TerminalBrokerSocketDir(manifest), binding)
}

func terminalBrokerDeadPath(path string) string { return path + ".dead" }

func ValidTerminalBrokerBinding(binding TerminalBrokerBinding) bool {
	secret, err := hex.DecodeString(binding.Secret)
	return err == nil && len(secret) == 32 && binding.Version == terminalBrokerVersion && binding.OuterPID >= 2 &&
		binding.InnerPID >= 2 && binding.InnerPGID == binding.InnerPID && filepath.IsAbs(binding.SocketPath) &&
		filepath.Clean(binding.SocketPath) == binding.SocketPath && binding.SocketDev != 0 && binding.SocketIno != 0
}

func ReadTerminalBrokerBinding(path string) (TerminalBrokerBinding, error) {
	body, err := readImmutableIdentity(path)
	if err != nil {
		return TerminalBrokerBinding{}, err
	}
	var binding TerminalBrokerBinding
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&binding) != nil || decoder.Decode(&struct{}{}) != io.EOF || !ValidTerminalBrokerBinding(binding) {
		return TerminalBrokerBinding{}, errors.New("terminal broker binding is invalid")
	}
	return binding, nil
}

func writeTerminalBrokerBinding(path string, binding TerminalBrokerBinding) error {
	if !ValidTerminalBrokerBinding(binding) {
		return errors.New("terminal broker binding is invalid")
	}
	body, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	return writeImmutableImplementationGroup(path, body)
}

func TerminalBrokerDead(path string, binding TerminalBrokerBinding) (bool, error) {
	body, err := readImmutableIdentity(terminalBrokerDeadPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	want, err := json.Marshal(binding)
	return err == nil && bytes.Equal(body, want), err
}

// TerminalBrokerOuterDead proves only that the bound outer process terminated.
// Callers must separately prove inner-group death and validate the pane binding.
func TerminalBrokerOuterDead(binding TerminalBrokerBinding) (bool, error) {
	if !ValidTerminalBrokerBinding(binding) {
		return false, errors.New("terminal broker binding is invalid")
	}
	return terminalProcessExited(binding.OuterPID)
}

type terminalBrokerClientState struct {
	conn      *net.UnixConn
	notify    chan struct{}
	pending   []byte
	inFlight  int
	replaying bool
	closed    bool
}

type terminalBroker struct {
	binding TerminalBrokerBinding
	record  string
	master  *os.File
	gate    *os.File

	mu         sync.Mutex
	replay     []byte
	overflow   bool
	draining   bool
	clients    map[*terminalBrokerClientState]struct{}
	inputMu    sync.Mutex
	signalMu   sync.Mutex
	release    sync.Once
	releaseErr error
	stop       sync.Once
	finish     sync.Once
	innerEnd   chan struct{}
	stopped    chan struct{}
	stopCall   chan struct{}
	stopAck    chan struct{}
}

// RunTerminalBroker owns the only handle used to stop and reap the exact
// inner session. The child remains behind a pipe gate until Release is called.
func RunTerminalBroker(ctx context.Context, recordPath, socketDir string, command []string, outerIn *os.File, outerOut, stderr io.Writer, started func(TerminalBrokerBinding) error) (int, syscall.Signal, error) {
	if !filepath.IsAbs(recordPath) || filepath.Clean(recordPath) != recordPath || !filepath.IsAbs(socketDir) || filepath.Clean(socketDir) != socketDir || len(command) == 0 || command[0] == "" || outerIn == nil || outerOut == nil {
		return 125, 0, errors.New("terminal broker invocation is invalid")
	}
	info, err := os.Lstat(filepath.Dir(recordPath))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !runtimeOwnedByCurrentUser(info) {
		return 125, 0, errors.New("terminal broker directory is unsafe")
	}
	info, err = os.Lstat(socketDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !runtimeOwnedByCurrentUser(info) {
		return 125, 0, errors.New("terminal broker socket directory is unsafe")
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return 125, 0, err
	}
	socketPath := TerminalBrokerSocketPath(recordPath, socketDir)
	if len(socketPath) > 100 {
		return 125, 0, errors.New("terminal broker socket path is too long")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return 125, 0, err
	}
	defer listener.Close()
	defer os.Remove(socketPath)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return 125, 0, err
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 {
		return 125, 0, errors.New("terminal broker socket is unsafe")
	}
	dev, ino, err := terminalSocketIdentity(socketInfo)
	if err != nil {
		return 125, 0, err
	}
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		return 125, 0, err
	}
	defer gateReader.Close()
	defer gateWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		return 125, 0, err
	}
	defer holdReader.Close()
	defer holdWriter.Close()
	inner := exec.Command("/bin/sh", append([]string{"-c", terminalBrokerWrapper, "terminal-broker"}, command...)...)
	inner.ExtraFiles = []*os.File{gateReader, holdReader}
	size := &pty.Winsize{Rows: 24, Cols: 80}
	if current, sizeErr := pty.GetsizeFull(outerIn); sizeErr == nil && current.Rows >= 2 && current.Cols >= 2 {
		size = current
	}
	master, err := pty.StartWithSize(inner, size)
	if err != nil {
		return 125, 0, err
	}
	_ = gateReader.Close()
	_ = holdReader.Close()
	innerPGID, err := syscall.Getpgid(inner.Process.Pid)
	if err != nil || innerPGID != inner.Process.Pid {
		stopUnverifiedTerminalBrokerChild(inner, gateWriter, holdWriter)
		return 125, 0, errors.New("terminal broker inner session identity is unavailable")
	}
	stopInnerGroup := func() error {
		killErr := syscall.Kill(-innerPGID, syscall.SIGKILL)
		_ = holdWriter.Close()
		terminated, groupErr := implementationGroupTerminated(innerPGID)
		if groupErr != nil || !terminated {
			return errors.Join(killErr, groupErr, errors.New("terminal broker inner group death is unproved"))
		}
		// Darwin reports EPERM for some zombie-only groups. Exact inventory has
		// already proved that such a group has no remaining execution authority.
		if killErr != nil && !errors.Is(killErr, syscall.ESRCH) && !errors.Is(killErr, syscall.EPERM) {
			return killErr
		}
		return nil
	}
	binding := TerminalBrokerBinding{Version: terminalBrokerVersion, OuterPID: os.Getpid(), InnerPID: inner.Process.Pid, InnerPGID: innerPGID, SocketPath: socketPath, SocketDev: dev, SocketIno: ino, Secret: hex.EncodeToString(secretBytes)}
	if err := writeTerminalBrokerBinding(recordPath, binding); err != nil {
		_ = stopInnerGroup()
		_ = inner.Wait()
		return 125, 0, err
	}
	if started != nil {
		if startedErr := started(binding); startedErr != nil {
			cleanupErr := stopInnerGroup()
			if waitErr := inner.Wait(); waitErr != nil {
				var exit *exec.ExitError
				if !errors.As(waitErr, &exit) {
					cleanupErr = errors.Join(cleanupErr, waitErr)
				}
			}
			if cleanupErr == nil {
				deadBody, _ := json.Marshal(binding)
				cleanupErr = writeImmutableImplementationGroup(terminalBrokerDeadPath(recordPath), deadBody)
			}
			return 125, 0, errors.Join(startedErr, cleanupErr)
		}
	}
	broker := &terminalBroker{binding: binding, record: recordPath, master: master, gate: gateWriter, clients: map[*terminalBrokerClientState]struct{}{}, innerEnd: make(chan struct{}), stopped: make(chan struct{}), stopCall: make(chan struct{}, 1), stopAck: make(chan struct{}, 1)}
	defer broker.finishStop()
	defer master.Close()
	defer broker.closeClients()
	go broker.accept(listener)
	go broker.copyOuterInput(outerIn)
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		broker.copyOutput(outerOut)
	}()
	go func() {
		<-ctx.Done()
		broker.killInner()
	}()
	if err := terminalMakeRaw(outerIn); err == nil {
		defer terminalRestore(outerIn)
	}
	resize := make(chan os.Signal, 1)
	terminalNotifyResize(resize)
	defer terminalStopResize(resize)
	go func() {
		for range resize {
			if current, sizeErr := pty.GetsizeFull(outerIn); sizeErr == nil {
				_ = pty.Setsize(master, current)
			}
		}
	}()
	exitErr := terminalWaitProcessExit(inner.Process.Pid)
	broker.signalMu.Lock()
	cleanupErr := errors.Join(exitErr, stopInnerGroup())
	waitErr := inner.Wait()
	close(broker.innerEnd)
	broker.signalMu.Unlock()
	if cleanupErr == nil {
		select {
		case <-outputDone:
			broker.mu.Lock()
			broker.draining = true
			broker.mu.Unlock()
			broker.waitClientDrain(terminalOutputDrainWait)
		case <-time.After(terminalOutputDrainWait):
			cleanupErr = errors.New("terminal broker output drain is unproved")
		}
	}
	if cleanupErr == nil {
		deadBody, _ := json.Marshal(binding)
		cleanupErr = writeImmutableImplementationGroup(terminalBrokerDeadPath(recordPath), deadBody)
	}
	broker.finishStop()
	select {
	case <-broker.stopCall:
		select {
		case <-broker.stopAck:
		case <-time.After(5 * time.Second):
			return 125, 0, errors.New("terminal broker stop response was not delivered")
		}
	default:
	}
	if cleanupErr != nil {
		return 125, 0, cleanupErr
	}
	status, ok := inner.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		return 125, 0, errors.New("terminal broker child wait status is unavailable")
	}
	if status.Signaled() {
		return 128 + int(status.Signal()), status.Signal(), nil
	}
	code := status.ExitStatus()
	if waitErr != nil {
		var exit *exec.ExitError
		if !errors.As(waitErr, &exit) {
			return code, 0, waitErr
		}
	}
	return code, 0, nil
}

func (b *terminalBroker) copyOutput(outer io.Writer) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := b.master.Read(buffer)
		if n > 0 {
			chunk := bytes.Clone(buffer[:n])
			_, _ = outer.Write(chunk)
			b.mu.Lock()
			if !b.overflow {
				if len(b.replay)+len(chunk) <= terminalReplayLimit {
					b.replay = append(b.replay, chunk...)
				} else {
					b.overflow = true
					b.replay = nil
				}
			}
			for client := range b.clients {
				if client.inFlight+len(client.pending)+len(chunk) > terminalClientQueueLimit {
					delete(b.clients, client)
					client.closed = true
					close(client.notify)
					_ = client.conn.Close()
					continue
				}
				client.pending = append(client.pending, chunk...)
				select {
				case client.notify <- struct{}{}:
				default:
				}
			}
			b.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (b *terminalBroker) copyOuterInput(outer io.Reader) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := outer.Read(buffer)
		if n > 0 {
			b.inputMu.Lock()
			_, _ = b.master.Write(buffer[:n])
			b.inputMu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (b *terminalBroker) accept(listener *net.UnixListener) {
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		go b.handle(conn)
	}
}

func (b *terminalBroker) handle(conn *net.UnixConn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	op, payload, err := readTerminalFrame(conn)
	if err != nil || op != terminalFrameAuth {
		return
	}
	var request terminalBrokerRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || request.Version != terminalBrokerVersion || request.Secret != b.binding.Secret {
		return
	}
	switch request.Action {
	case "release":
		if err := b.releaseInner(); err != nil {
			_ = writeTerminalStatus(conn, terminalBrokerStatus{Error: "release failed"})
			return
		}
		_ = writeTerminalStatus(conn, terminalBrokerStatus{OK: true})
	case "stop":
		select {
		case b.stopCall <- struct{}{}:
		default:
		}
		b.killInner()
		<-b.stopped
		dead, err := TerminalBrokerDead(b.record, b.binding)
		status := terminalBrokerStatus{OK: dead}
		if err != nil || !dead {
			status.Error = "terminal broker inner group death is unproved"
		}
		if writeTerminalStatus(conn, status) == nil {
			select {
			case b.stopAck <- struct{}{}:
			default:
			}
		}
	case "attach":
		client := &terminalBrokerClientState{conn: conn, notify: make(chan struct{}, 1)}
		b.mu.Lock()
		overflow := b.overflow
		draining := b.draining
		var replay []byte
		if !overflow && !draining {
			replay = bytes.Clone(b.replay)
			client.replaying = len(replay) != 0
			b.clients[client] = struct{}{}
		}
		b.mu.Unlock()
		if overflow {
			_ = writeTerminalStatus(conn, terminalBrokerStatus{Overflow: true, Error: "terminal replay limit exceeded"})
			return
		}
		if draining {
			_ = writeTerminalStatus(conn, terminalBrokerStatus{Error: "terminal broker is closing"})
			return
		}
		removeClient := func() {
			b.mu.Lock()
			if !client.closed {
				delete(b.clients, client)
				client.closed = true
				close(client.notify)
			}
			b.mu.Unlock()
		}
		if writeTerminalStatus(conn, terminalBrokerStatus{OK: true}) != nil {
			removeClient()
			return
		}
		_ = conn.SetDeadline(time.Time{})
		op, payload, err = readTerminalFrame(conn)
		if err != nil || op != terminalFrameStart || len(payload) != 0 {
			removeClient()
			return
		}
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			for len(replay) != 0 {
				n := min(len(replay), terminalFrameLimit)
				if writeTerminalFrame(conn, terminalFrameOutput, replay[:n]) != nil {
					return
				}
				replay = replay[n:]
			}
			b.mu.Lock()
			client.replaying = false
			b.mu.Unlock()
			for {
				b.mu.Lock()
				if client.closed {
					b.mu.Unlock()
					return
				}
				if len(client.pending) == 0 {
					b.mu.Unlock()
					<-client.notify
					continue
				}
				chunk := client.pending
				client.pending = nil
				client.inFlight = len(chunk)
				b.mu.Unlock()
				if writeTerminalFrame(conn, terminalFrameOutput, chunk) != nil {
					return
				}
				b.mu.Lock()
				client.inFlight = 0
				b.mu.Unlock()
			}
		}()
		for {
			op, payload, err = readTerminalFrame(conn)
			if err != nil {
				break
			}
			switch op {
			case terminalFrameInput:
				b.inputMu.Lock()
				_, err = b.master.Write(payload)
				b.inputMu.Unlock()
			case terminalFrameResize:
				var resize terminalBrokerResize
				if json.Unmarshal(payload, &resize) != nil || resize.Cols < 2 || resize.Rows < 2 || resize.Cols > 500 || resize.Rows > 300 {
					err = errors.New("invalid terminal resize")
				} else {
					err = pty.Setsize(b.master, &pty.Winsize{Cols: resize.Cols, Rows: resize.Rows})
				}
			default:
				err = errors.New("invalid terminal broker frame")
			}
			if err != nil {
				break
			}
		}
		removeClient()
		_ = conn.Close()
		<-writerDone
	}
}

func (b *terminalBroker) releaseInner() error {
	b.release.Do(func() {
		_, b.releaseErr = io.WriteString(b.gate, "go\n")
		_ = b.gate.Close()
	})
	return b.releaseErr
}

func (b *terminalBroker) killInner() {
	b.stop.Do(func() {
		b.signalMu.Lock()
		select {
		case <-b.innerEnd:
			b.signalMu.Unlock()
			return
		default:
		}
		_ = syscall.Kill(-b.binding.InnerPGID, syscall.SIGTERM)
		_ = b.gate.Close()
		b.signalMu.Unlock()
		go func() {
			select {
			case <-b.innerEnd:
			case <-time.After(2 * time.Second):
				b.signalMu.Lock()
				defer b.signalMu.Unlock()
				select {
				case <-b.innerEnd:
					return
				default:
					_ = syscall.Kill(-b.binding.InnerPGID, syscall.SIGKILL)
				}
			}
		}()
	})
}

func (b *terminalBroker) finishStop() {
	b.finish.Do(func() { close(b.stopped) })
}

func (b *terminalBroker) closeClients() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for client := range b.clients {
		delete(b.clients, client)
		client.closed = true
		close(client.notify)
		_ = client.conn.Close()
	}
}

func (b *terminalBroker) waitClientDrain(timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		b.mu.Lock()
		drained := true
		for client := range b.clients {
			if client.replaying || client.inFlight != 0 || len(client.pending) != 0 {
				drained = false
				break
			}
		}
		b.mu.Unlock()
		if drained {
			return
		}
		select {
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

type TerminalBrokerClient struct {
	conn *net.UnixConn
	mu   sync.Mutex
}

func DialTerminalBroker(ctx context.Context, binding TerminalBrokerBinding) (*TerminalBrokerClient, error) {
	if err := verifyTerminalBrokerSocket(binding); err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "unix", binding.SocketPath)
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*net.UnixConn)
	if !ok {
		raw.Close()
		return nil, errors.New("terminal broker connection is not unix")
	}
	fail := func(err error) (*TerminalBrokerClient, error) {
		conn.Close()
		return nil, err
	}
	if err := verifyTerminalPeerPID(conn, binding.OuterPID); err != nil {
		return fail(err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	request, _ := json.Marshal(terminalBrokerRequest{Version: terminalBrokerVersion, Secret: binding.Secret, Action: "attach"})
	if err := writeTerminalFrame(conn, terminalFrameAuth, request); err != nil {
		return fail(err)
	}
	status, err := readTerminalStatus(conn)
	if err != nil || !status.OK {
		if status.Overflow {
			err = errors.New("terminal replay limit exceeded")
		} else if err == nil {
			err = errors.New("terminal broker rejected attachment")
		}
		return fail(err)
	}
	_ = conn.SetDeadline(time.Time{})
	return &TerminalBrokerClient{conn: conn}, nil
}

func (c *TerminalBrokerClient) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeTerminalFrame(c.conn, terminalFrameStart, nil)
}

func (c *TerminalBrokerClient) ReadOutput() ([]byte, error) {
	op, payload, err := readTerminalFrame(c.conn)
	if err != nil {
		return nil, err
	}
	if op != terminalFrameOutput {
		return nil, errors.New("terminal broker sent an invalid frame")
	}
	return payload, nil
}

func (c *TerminalBrokerClient) WriteInput(payload []byte) error {
	if len(payload) > terminalFrameLimit {
		return errors.New("terminal input exceeds limit")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeTerminalFrame(c.conn, terminalFrameInput, payload)
}

func (c *TerminalBrokerClient) Resize(cols, rows uint16) error {
	payload, _ := json.Marshal(terminalBrokerResize{Cols: cols, Rows: rows})
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeTerminalFrame(c.conn, terminalFrameResize, payload)
}

func (c *TerminalBrokerClient) Close() error { return c.conn.Close() }

func ReleaseTerminalBroker(ctx context.Context, binding TerminalBrokerBinding) error {
	return terminalBrokerControl(ctx, binding, "release")
}

func StopTerminalBroker(ctx context.Context, binding TerminalBrokerBinding) error {
	return terminalBrokerControl(ctx, binding, "stop")
}

func StopAndProveTerminalBroker(ctx context.Context, recordPath string, binding TerminalBrokerBinding) error {
	if !ValidTerminalBrokerBinding(binding) {
		return errors.New("terminal broker binding is invalid")
	}
	dead, err := TerminalBrokerDead(recordPath, binding)
	if err != nil {
		return err
	}
	var stopErr error
	if !dead {
		stopCtx, cancel := context.WithTimeout(ctx, terminalBrokerStopWait)
		stopErr = StopTerminalBroker(stopCtx, binding)
		cancel()
	}
	deadline := time.NewTimer(terminalBrokerStopWait)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		dead, err = TerminalBrokerDead(recordPath, binding)
		if err != nil || dead {
			if !dead && err == nil {
				err = errors.New("terminal broker death is unproved")
			}
			if dead {
				return nil
			}
			return errors.Join(stopErr, err)
		}
		select {
		case <-ctx.Done():
			return errors.Join(stopErr, ctx.Err())
		case <-deadline.C:
			return errors.Join(stopErr, errors.New("terminal broker death is unproved"))
		case <-ticker.C:
		}
	}
}

func terminalBrokerControl(ctx context.Context, binding TerminalBrokerBinding, action string) error {
	if action != "release" && action != "stop" {
		return errors.New("invalid terminal broker control")
	}
	if err := verifyTerminalBrokerSocket(binding); err != nil {
		return err
	}
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "unix", binding.SocketPath)
	if err != nil {
		return err
	}
	defer raw.Close()
	conn, ok := raw.(*net.UnixConn)
	if !ok {
		return errors.New("terminal broker connection is not unix")
	}
	if err := verifyTerminalPeerPID(conn, binding.OuterPID); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	request, _ := json.Marshal(terminalBrokerRequest{Version: terminalBrokerVersion, Secret: binding.Secret, Action: action})
	if err := writeTerminalFrame(conn, terminalFrameAuth, request); err != nil {
		return err
	}
	status, err := readTerminalStatus(conn)
	if err != nil {
		return err
	}
	if !status.OK {
		if status.Error != "" {
			return errors.New(status.Error)
		}
		return errors.New("terminal broker rejected control")
	}
	return nil
}

func verifyTerminalBrokerSocket(binding TerminalBrokerBinding) error {
	if !ValidTerminalBrokerBinding(binding) {
		return errors.New("terminal broker binding is invalid")
	}
	info, err := os.Lstat(binding.SocketPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return errors.New("terminal broker socket is unavailable")
	}
	dev, ino, err := terminalSocketIdentity(info)
	if err != nil || dev != binding.SocketDev || ino != binding.SocketIno {
		return errors.New("terminal broker socket identity changed")
	}
	return nil
}

func writeTerminalStatus(w io.Writer, status terminalBrokerStatus) error {
	payload, _ := json.Marshal(status)
	return writeTerminalFrame(w, terminalFrameStatus, payload)
}

func readTerminalStatus(r io.Reader) (terminalBrokerStatus, error) {
	op, payload, err := readTerminalFrame(r)
	if err != nil || op != terminalFrameStatus {
		return terminalBrokerStatus{}, errors.Join(err, errors.New("terminal broker response is invalid"))
	}
	var status terminalBrokerStatus
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return terminalBrokerStatus{}, errors.New("terminal broker response is invalid")
	}
	return status, nil
}

func writeTerminalFrame(w io.Writer, op byte, payload []byte) error {
	if len(payload) > terminalReplayLimit {
		return errors.New("terminal broker frame exceeds limit")
	}
	header := [5]byte{op}
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readTerminalFrame(r io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	limit := uint32(terminalFrameLimit)
	if header[0] == terminalFrameOutput {
		limit = terminalReplayLimit
	}
	if size > limit {
		return 0, nil, errors.New("terminal broker frame exceeds limit")
	}
	payload := make([]byte, size)
	_, err := io.ReadFull(r, payload)
	return header[0], payload, err
}
