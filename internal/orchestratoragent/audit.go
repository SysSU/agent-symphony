package orchestratoragent

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	AuditLaunchMaxBytes = 128 << 10
	AuditOutputMaxBytes = 1 << 20
)

// AuditLaunch exists only in memory and on the launcher's private stdin pipe.
// The interactive orchestrator continues to use its separate file contract.
type AuditLaunch struct {
	Version int      `json:"version"`
	Command []string `json:"command"`
	Context string   `json:"context"`
	Timeout int      `json:"timeout_seconds"`
}

// parseAuditOutput consumes the complete bounded Codex exec --json stream.
// A successful exit alone cannot establish that the model completed its turn.
func parseAuditOutput(output string) (string, error) {
	if len(output) == 0 || len(output) > AuditOutputMaxBytes || !utf8.ValidString(output) || !strings.HasSuffix(output, "\n") {
		return "", errors.New("audit JSONL output is missing, truncated, or oversized")
	}
	report := ""
	completed := false
	started := false
	for line := range strings.SplitSeq(strings.TrimSuffix(output, "\n"), "\n") {
		var event struct {
			Type string `json:"type"`
			Item struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"item"`
		}
		if completed || json.Unmarshal([]byte(line), &event) != nil || event.Type == "" {
			return "", errors.New("invalid audit JSONL event or event after completion")
		}
		switch event.Type {
		case "error", "turn.failed":
			return "", errors.New("audit reported a failed turn")
		case "turn.started":
			if started || report != "" {
				return "", errors.New("audit returned multiple or out-of-order turns")
			}
			started = true
		case "item.completed":
			if event.Item.Type == "" || event.Item.Type == "error" {
				return "", errors.New("audit returned an invalid or failed item")
			}
			if event.Item.Type == "agent_message" {
				if strings.TrimSpace(event.Item.Text) == "" || len(event.Item.Text) > maxAuditReportBytes {
					return "", errors.New("audit final message is empty or oversized")
				}
				report = event.Item.Text
			}
		case "turn.completed":
			completed = true
		default:
			if strings.HasPrefix(event.Type, "turn.") {
				return "", errors.New("audit returned an unsupported turn outcome")
			}
		}
	}
	if !completed || report == "" {
		return "", errors.New("audit did not return a final message and completed turn")
	}
	return report, nil
}
