package main

import "syscall"

func readHostBootIdentity() hostBootIdentity {
	// XNU exposes a read-only, fixed-size UUID string for this boot session.
	value, err := syscall.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return hostBootIdentity{}
	}
	return parseHostBootIdentity("darwin-bootsessionuuid-v1", value)
}
