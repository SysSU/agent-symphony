package github

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	readCacheVersion  = 1
	maxReadCacheFile  = 64 << 20
	maxReadCacheBytes = 32 << 20
	maxReadCacheItems = 4096
)

type readCacheEntry struct {
	ETag string          `json:"etag"`
	Body json.RawMessage `json:"body"`
}

type ReadCache struct {
	path    string
	entries map[string]readCacheEntry
	used    map[string]bool
	size    int
	dirty   bool
}

func LoadReadCache(path string) (*ReadCache, error) {
	cache := &ReadCache{path: path, entries: map[string]readCacheEntry{}, used: map[string]bool{}}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return cache, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
		return nil, errors.New("GitHub ETag cache must be a regular non-symlink file with mode 0600")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, errors.New("GitHub ETag cache changed while opening")
	}
	body, err := io.ReadAll(io.LimitReader(f, maxReadCacheFile+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxReadCacheFile {
		return nil, errors.New("GitHub ETag cache exceeds 64 MiB limit")
	}
	var state struct {
		Version int                       `json:"version"`
		Entries map[string]readCacheEntry `json:"entries"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode GitHub ETag cache: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("GitHub ETag cache has trailing data")
	}
	if state.Version != readCacheVersion || state.Entries == nil || len(state.Entries) > maxReadCacheItems {
		return nil, errors.New("GitHub ETag cache has an unsupported or invalid structure")
	}
	for path, entry := range state.Entries {
		if err := validateReadCacheEntry(path, entry); err != nil {
			return nil, err
		}
		cache.size += readCacheEntrySize(path, entry)
	}
	if cache.size > maxReadCacheBytes {
		return nil, errors.New("GitHub ETag cache content exceeds 32 MiB limit")
	}
	cache.entries = state.Entries
	return cache, nil
}

func (c *ReadCache) get(path string) (readCacheEntry, bool) {
	entry, ok := c.entries[path]
	if ok {
		c.used[path] = true
	}
	return entry, ok
}

func (c *ReadCache) put(path, etag string, body []byte) error {
	c.used[path] = true
	old, present := c.entries[path]
	if etag == "" {
		if present {
			delete(c.entries, path)
			c.size -= readCacheEntrySize(path, old)
			c.dirty = true
		}
		return nil
	}
	entry := readCacheEntry{ETag: etag, Body: append(json.RawMessage(nil), body...)}
	if err := validateReadCacheEntry(path, entry); err != nil {
		return err
	}
	size := c.size + readCacheEntrySize(path, entry)
	if present {
		size -= readCacheEntrySize(path, old)
	}
	// ponytail: skip caching overflow entries; add eviction only if real repositories hit this ceiling.
	if size > maxReadCacheBytes || !present && len(c.entries) >= maxReadCacheItems {
		if present {
			delete(c.entries, path)
			c.size -= readCacheEntrySize(path, old)
			c.dirty = true
		}
		return nil
	}
	if !present || old.ETag != entry.ETag || !bytes.Equal(old.Body, entry.Body) {
		c.entries[path] = entry
		c.size = size
		c.dirty = true
	}
	return nil
}

func (c *ReadCache) Save() error {
	for path, entry := range c.entries {
		if c.used[path] {
			continue
		}
		delete(c.entries, path)
		c.size -= readCacheEntrySize(path, entry)
		c.dirty = true
	}
	if !c.dirty {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err != nil {
		return err
	}
	if info, err := os.Lstat(c.path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("GitHub ETag cache must be a regular non-symlink file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	body, err := json.MarshalIndent(struct {
		Version int                       `json:"version"`
		Entries map[string]readCacheEntry `json:"entries"`
	}{readCacheVersion, c.entries}, "", "  ")
	if err != nil {
		return err
	}
	if len(body) > maxReadCacheFile {
		return errors.New("GitHub ETag cache exceeds 64 MiB limit")
	}
	tmp, err := os.CreateTemp(filepath.Dir(c.path), ".github-etags-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(append(body, '\n'))
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(name, c.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(c.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func validateReadCacheEntry(path string, entry readCacheEntry) error {
	if !strings.HasPrefix(path, "/") || len(path) > 4096 || entry.ETag == "" || len(entry.ETag) > 1024 || strings.ContainsAny(entry.ETag, "\r\n") || len(entry.Body) > maxJSONBody || !json.Valid(entry.Body) {
		return errors.New("GitHub ETag cache contains an invalid entry")
	}
	return nil
}

func readCacheEntrySize(path string, entry readCacheEntry) int {
	return len(path) + len(entry.ETag) + len(entry.Body)
}
