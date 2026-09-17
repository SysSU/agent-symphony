//go:build agent_symphony_test

package config

func init() {
	testNativeWorkerExecutableAllowed = func(string) bool { return true }
}
