package github

import (
	"errors"
	"regexp"
	"strings"
)

var secretPattern = regexp.MustCompile(`(?i)(authorization|token|secret|password|passwd|private[_-]?key|api[_-]?key|github_pat)(\s*[:=]\s*|\s+)[^\s,;"']+`)

var githubCLIEnvironment = []string{"GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN", "GH_HOST", "GH_REPO", "GH_CONFIG_DIR"}

func Redact(value string, known ...string) string {
	for _, secret := range known {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return secretPattern.ReplaceAllString(value, "$1$2[REDACTED]")
}

// RedactEnvironment removes credential values carried in an agent environment.
func RedactEnvironment(value string, environment []string) string {
	var known []string
	for _, entry := range environment {
		name, secret, ok := strings.Cut(entry, "=")
		if ok && sensitiveEnvironmentVariable(name) {
			known = append(known, secret)
		}
	}
	return Redact(value, known...)
}

func AgentEnvironment(environment []string) []string {
	result, _ := AgentEnvironmentWith(environment)
	return result
}

func AgentEnvironmentWith(environment []string, allowed ...string) ([]string, error) {
	safe := map[string]bool{"PATH": true, "TMPDIR": true, "LANG": true, "LC_ALL": true, "TERM": true, "COLORTERM": true, "NO_COLOR": true, "CODEX_HOME": true}
	for _, name := range githubCLIEnvironment {
		safe[name] = true
	}
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

func sensitiveEnvironmentVariable(name string) bool {
	return modelCredentialVariable(name) || name == "GH_TOKEN" || name == "GITHUB_TOKEN" || name == "GH_ENTERPRISE_TOKEN" || name == "GITHUB_ENTERPRISE_TOKEN"
}

func reservedAgentVariable(name string) bool {
	if GitHubCLIEnvironmentVariable(name) {
		return false
	}
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

// GitHubCLIEnvironmentVariable reports variables used by gh to find its
// authenticated session and target repository without copying credentials to
// repository files.
func GitHubCLIEnvironmentVariable(name string) bool {
	for _, allowed := range githubCLIEnvironment {
		if name == allowed {
			return true
		}
	}
	return false
}

// GitHubCLIEnvironmentNames returns the bounded environment allowlist used by gh.
func GitHubCLIEnvironmentNames() []string { return append([]string(nil), githubCLIEnvironment...) }
