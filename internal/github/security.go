package github

import (
	"errors"
	"regexp"
	"strings"
)

var secretPattern = regexp.MustCompile(`(?i)(authorization|token|secret|password|passwd|private[_-]?key|api[_-]?key|github_pat)(\s*[:=]\s*|\s+)[^\s,;"']+`)

func Redact(value string, known ...string) string {
	for _, secret := range known {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return secretPattern.ReplaceAllString(value, "$1$2[REDACTED]")
}

func AgentEnvironment(environment []string) []string {
	result, _ := AgentEnvironmentWith(environment)
	return result
}

func AgentEnvironmentWith(environment []string, allowed ...string) ([]string, error) {
	safe := map[string]bool{"PATH": true, "TMPDIR": true, "LANG": true, "LC_ALL": true, "TERM": true, "COLORTERM": true, "NO_COLOR": true}
	for _, name := range allowed {
		if reservedAgentVariable(name) && !modelCredentialVariable(name) {
			return nil, errors.New("reserved credential or coordinator variable " + name + " cannot be allowed")
		}
		safe[name] = true
	}
	result := make([]string, 0, len(environment)+3)
	for _, entry := range environment {
		name := strings.SplitN(entry, "=", 2)[0]
		if safe[name] {
			result = append(result, entry)
		}
	}
	return append(result, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=credential.helper", "GIT_CONFIG_VALUE_0="), nil
}

func modelCredentialVariable(name string) bool {
	switch name {
	case "MODEL_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "CODEX_API_KEY":
		return true
	}
	return false
}

func reservedAgentVariable(name string) bool {
	upper := strings.ToUpper(name)
	if upper == "HOME" {
		return true
	}
	for _, prefix := range []string{"GITHUB_", "GH_", "SSH_", "AWS_", "AZURE_", "GOOGLE_", "GCP_", "CLOUD_", "OCI_", "CLOUDFLARE_", "DIGITALOCEAN_", "GIT_ASKPASS", "GIT_CONFIG", "APP_"} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	if strings.HasSuffix(upper, "_PROXY") {
		return true
	}
	for _, part := range []string{"TOKEN", "SECRET", "PASSWORD", "PASSWD", "PRIVATE_KEY", "PRIVATE-KEY", "CREDENTIAL", "API_KEY", "API-KEY", "ACCESS_KEY", "APP_KEY", "AUTHORIZATION", "GITHUB_PAT"} {
		if strings.Contains(upper, part) {
			return true
		}
	}
	return false
}
