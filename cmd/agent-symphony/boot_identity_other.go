//go:build !darwin && !linux

package main

func readHostBootIdentity() hostBootIdentity { return hostBootIdentity{} }
