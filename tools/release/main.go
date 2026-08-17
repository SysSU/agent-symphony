// Command release creates byte-reproducible, runtime-independent release archives.
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var targets = [][2]string{{"darwin", "amd64"}, {"darwin", "arm64"}, {"linux", "amd64"}, {"linux", "arm64"}}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "-verify" {
		must(verify(os.Args[2]))
		return
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: release VERSION OUTPUT_DIR | release -verify OUTPUT_DIR")
		os.Exit(2)
	}
	must(build(os.Args[1], os.Args[2]))
}

func build(version, out string) error {
	epoch, err := strconv.ParseInt(os.Getenv("SOURCE_DATE_EPOCH"), 10, 64)
	if err != nil {
		return fmt.Errorf("SOURCE_DATE_EPOCH: %w", err)
	}
	stamp := time.Unix(epoch, 0).UTC()
	var sums []string
	for _, target := range targets {
		name := fmt.Sprintf("agent-symphony_%s_%s_%s", version, target[0], target[1])
		binary := filepath.Join(out, name, "agent-symphony")
		if err := os.MkdirAll(filepath.Dir(binary), 0755); err != nil {
			return err
		}
		cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-buildid= -X main.releaseMetadata=agent-symphony-release-version:"+version, "-o", binary, "./cmd/agent-symphony")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+target[0], "GOARCH="+target[1])
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			return err
		}
		archive := filepath.Join(out, name+".tar.gz")
		if err := pack(archive, name+"/agent-symphony", binary, stamp); err != nil {
			return err
		}
		if err := os.RemoveAll(filepath.Dir(binary)); err != nil {
			return err
		}
		sum, err := digest(archive)
		if err != nil {
			return err
		}
		sums = append(sums, sum+"  "+filepath.Base(archive))
	}
	sort.Strings(sums)
	return os.WriteFile(filepath.Join(out, "SHA256SUMS"), []byte(strings.Join(sums, "\n")+"\n"), 0644)
}

func pack(dst, name, src string, stamp time.Time) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
	}()
	gz, err := gzip.NewWriterLevel(out, gzip.BestCompression)
	if err != nil {
		return err
	}
	gz.Header.ModTime, gz.Header.OS = stamp, 255
	if err = writeTarHeader(gz, name, info.Size(), stamp); err == nil {
		_, err = io.Copy(gz, in)
	}
	if err == nil {
		padding := (512 - info.Size()%512) % 512
		_, err = gz.Write(make([]byte, padding+1024))
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writeTarHeader(w io.Writer, name string, size int64, stamp time.Time) error {
	if len(name) > 100 {
		return fmt.Errorf("archive path too long: %s", name)
	}
	h := make([]byte, 512)
	copy(h[0:100], name)
	octal := func(offset, width int, value int64) {
		copy(h[offset:offset+width], fmt.Sprintf("%0*o\x00", width-1, value))
	}
	octal(100, 8, 0755)
	octal(108, 8, 0)
	octal(116, 8, 0)
	octal(124, 12, size)
	octal(136, 12, stamp.Unix())
	for i := 148; i < 156; i++ {
		h[i] = ' '
	}
	h[156] = '0'
	copy(h[257:263], "ustar\x00")
	copy(h[263:265], "00")
	var sum int64
	for _, b := range h {
		sum += int64(b)
	}
	octal(148, 8, sum)
	_, err := w.Write(h)
	return err
}

func verify(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != len(targets) {
		return fmt.Errorf("checksum manifest must contain exactly %d archives", len(targets))
	}
	seen := make(map[string]bool, len(targets))
	version := ""
	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return fmt.Errorf("invalid checksum line %q", line)
		}
		name := parts[1]
		if filepath.Base(name) != name || filepath.IsAbs(name) || seen[name] {
			return fmt.Errorf("unsafe or duplicate archive name %q", name)
		}
		matchedVersion, ok := releaseArchiveVersion(name)
		if !ok || version != "" && matchedVersion != version {
			return fmt.Errorf("unexpected archive name %q", name)
		}
		version = matchedVersion
		seen[name] = true
		got, err := digest(filepath.Join(dir, parts[1]))
		if err != nil {
			return err
		}
		if got != parts[0] {
			return fmt.Errorf("checksum mismatch: %s", parts[1])
		}
		for _, target := range targets {
			if strings.HasSuffix(name, fmt.Sprintf("_%s_%s.tar.gz", target[0], target[1])) {
				if err := verifyArchive(filepath.Join(dir, name), strings.TrimSuffix(name, ".tar.gz")+"/agent-symphony", target[0], target[1], version); err != nil {
					return fmt.Errorf("%s: %w", name, err)
				}
			}
		}
	}
	for _, target := range targets {
		name := fmt.Sprintf("agent-symphony_%s_%s_%s.tar.gz", version, target[0], target[1])
		if !seen[name] {
			return fmt.Errorf("missing archive %q", name)
		}
	}
	return nil
}

func verifyArchive(path, expectedName, goos, goarch, version string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buffered := bufio.NewReader(f)
	gz, err := gzip.NewReader(buffered)
	if err != nil {
		return fmt.Errorf("invalid gzip: %w", err)
	}
	gz.Multistream(false)
	counted := &countingReader{Reader: gz}
	tr := tar.NewReader(counted)
	header, err := tr.Next()
	if err != nil {
		return fmt.Errorf("invalid tar: %w", err)
	}
	if header.Name != expectedName || header.Typeflag != tar.TypeReg || header.Mode != 0o755 || header.Size <= 0 || header.Size > 256<<20 {
		return fmt.Errorf("expected one regular executable %q with mode 0755 and size 1..268435456", expectedName)
	}
	tmp, err := os.CreateTemp("", "agent-symphony-verify-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err := io.Copy(tmp, tr); err != nil {
		return fmt.Errorf("invalid executable payload: %w", err)
	}
	info, err := buildinfo.Read(tmp)
	if err != nil {
		return fmt.Errorf("not a Go executable: %w", err)
	}
	if err := verifyBuildInfo(info, goos, goarch, version); err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	payload, err := io.ReadAll(tmp)
	if err != nil || !bytes.Contains(payload, []byte("agent-symphony-release-version:"+version)) {
		return fmt.Errorf("go executable lacks release version metadata %q", version)
	}
	if _, err := tr.Next(); err != io.EOF {
		if err == nil {
			return errors.New("archive contains more than one entry")
		}
		return fmt.Errorf("invalid tar trailer: %w", err)
	}
	if _, err := io.Copy(io.Discard, counted); err != nil {
		return fmt.Errorf("invalid gzip trailer: %w", err)
	}
	expectedSize := int64(512) + (header.Size+511)/512*512 + 1024
	if counted.N != expectedSize {
		return fmt.Errorf("tar contains trailing data: got %d bytes, expected %d", counted.N, expectedSize)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("invalid gzip trailer: %w", err)
	}
	if _, err := buffered.Peek(1); err != io.EOF {
		return errors.New("archive contains trailing compressed data")
	}
	return nil
}

type countingReader struct {
	io.Reader
	N int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.N += int64(n)
	return n, err
}

func verifyBuildInfo(info *buildinfo.BuildInfo, goos, goarch, version string) error {
	if info.Path != "github.com/SysSU/agent-symphony/cmd/agent-symphony" || info.Main.Path != "github.com/SysSU/agent-symphony" {
		return fmt.Errorf("unexpected Go command/module %q/%q", info.Path, info.Main.Path)
	}
	want := map[string]string{"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"}
	for _, setting := range info.Settings {
		if expected, ok := want[setting.Key]; ok && setting.Value == expected {
			delete(want, setting.Key)
		}
	}
	if len(want) != 0 {
		return fmt.Errorf("go build metadata does not match release %s for %s/%s", version, goos, goarch)
	}
	return nil
}

func releaseArchiveVersion(name string) (string, bool) {
	const prefix = "agent-symphony_"
	for _, target := range targets {
		suffix := fmt.Sprintf("_%s_%s.tar.gz", target[0], target[1])
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			version := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
			return version, version != "" && !strings.ContainsAny(version, `/\\`)
		}
	}
	return "", false
}

func digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
