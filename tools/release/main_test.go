package main

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPackIsReproducibleAndVerifiable(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "binary")
	if err := os.WriteFile(src, []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(0, 0).UTC()
	a, b := filepath.Join(dir, "a.tar.gz"), filepath.Join(dir, "b.tar.gz")
	if err := pack(a, "release/binary", src, stamp); err != nil {
		t.Fatal(err)
	}
	if err := pack(b, "release/binary", src, stamp); err != nil {
		t.Fatal(err)
	}
	one, _ := os.ReadFile(a)
	two, _ := os.ReadFile(b)
	if string(one) != string(two) {
		t.Fatal("archives differ")
	}
}

func TestVerifyRequiresExactSafeReleaseSet(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(filepath.Join(wd, "../..")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	makeRelease := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		t.Setenv("SOURCE_DATE_EPOCH", "0")
		if err := build("1.2.3", dir); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	baseDir := makeRelease(t)
	if err := verify(baseDir); err != nil {
		t.Fatal(err)
	}
	copyRelease := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		entries, err := os.ReadDir(baseDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			src, dst := filepath.Join(baseDir, entry.Name()), filepath.Join(dir, entry.Name())
			if entry.Name() == "SHA256SUMS" {
				b, _ := os.ReadFile(src)
				err = os.WriteFile(dst, b, 0o600)
			} else {
				err = os.Link(src, dst)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	for _, test := range []struct{ name, replacement string }{
		{"absolute", "/tmp/archive"}, {"traversal", "../archive"},
		{"duplicate", "agent-symphony_1.2.3_darwin_amd64.tar.gz"},
		{"missing", ""}, {"extra", "extra.tar.gz"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := copyRelease(t)
			path := filepath.Join(dir, "SHA256SUMS")
			b, _ := os.ReadFile(path)
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			switch test.name {
			case "missing":
				lines = lines[:3]
			case "extra":
				lines = append(lines, lines[0][:64]+"  "+test.replacement)
			default:
				lines[3] = lines[3][:64] + "  " + test.replacement
			}
			if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := verify(dir); err == nil {
				t.Fatal("verify accepted invalid manifest")
			}
		})
	}
}

func executableHeader(goos, goarch string) []byte {
	b := make([]byte, 20)
	if goos == "linux" {
		copy(b, "\x7fELF")
		b[4], b[5] = 2, 1
	} else {
		copy(b, "\xcf\xfa\xed\xfe")
	}
	return b
}

func TestVerifyArchiveRejectsUnsafePayloads(t *testing.T) {
	write := func(t *testing.T, entries []tar.Header, bodies [][]byte, suffix []byte) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "archive.tar.gz")
		f, _ := os.Create(path)
		gz := gzip.NewWriter(f)
		tw := tar.NewWriter(gz)
		for i := range entries {
			if err := tw.WriteHeader(&entries[i]); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write(bodies[i]); err != nil {
				t.Fatal(err)
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		if len(suffix) > 0 {
			_, _ = gz.Write(suffix)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}
	good := executableHeader("linux", "amd64")
	base := tar.Header{Name: "release/binary", Mode: 0o755, Size: int64(len(good)), Typeflag: tar.TypeReg}
	for _, tc := range []struct {
		name    string
		headers []tar.Header
		bodies  [][]byte
		suffix  []byte
	}{
		{"link", []tar.Header{{Name: "release/binary", Mode: 0o755, Typeflag: tar.TypeSymlink, Linkname: "elsewhere"}}, [][]byte{{}}, nil},
		{"wrong mode", []tar.Header{{Name: base.Name, Mode: 0o644, Size: int64(len(good)), Typeflag: tar.TypeReg}}, [][]byte{good}, nil},
		{"truncated executable", []tar.Header{base}, [][]byte{good}, nil},
		{"inert executable", []tar.Header{{Name: base.Name, Mode: 0o755, Size: 20, Typeflag: tar.TypeReg}}, [][]byte{[]byte("#!/bin/sh\nexit 0\n   ")}, nil},
		{"wrong architecture", []tar.Header{base}, [][]byte{executableHeader("linux", "arm64")}, nil},
		{"extra entry", []tar.Header{base, {Name: "extra", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg}}, [][]byte{good, []byte("x")}, nil},
		{"trailing tar data", []tar.Header{base}, [][]byte{good}, []byte("garbage")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := verifyArchive(write(t, tc.headers, tc.bodies, tc.suffix), "release/binary", "linux", "amd64", "1.2.3"); err == nil {
				t.Fatal("accepted unsafe archive")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "not-gzip")
	_ = os.WriteFile(path, []byte("not an archive"), 0o600)
	if err := verifyArchive(path, "release/binary", "linux", "amd64", "1.2.3"); err == nil {
		t.Fatal("accepted malformed payload")
	}
	path = write(t, []tar.Header{base}, [][]byte{good}, nil)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write([]byte("garbage"))
	_ = f.Close()
	if err := verifyArchive(path, "release/binary", "linux", "amd64", "1.2.3"); err == nil {
		t.Fatal("accepted trailing compressed garbage")
	}
}
