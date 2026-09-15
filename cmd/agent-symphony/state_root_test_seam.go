//go:build agent_symphony_test

package main

func init() {
	testRuntimeStateRootAllowed = func(string) bool { return true }
}
