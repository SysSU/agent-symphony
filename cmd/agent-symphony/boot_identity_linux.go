package main

import (
	"io"
	"os"
	"strings"
)

func readHostBootIdentity() hostBootIdentity {
	file, err := os.Open("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return hostBootIdentity{}
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, 38))
	if err != nil {
		return hostBootIdentity{}
	}
	return parseHostBootIdentity("linux-boot-id-v1", strings.TrimSuffix(string(value), "\n"))
}
