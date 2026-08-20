package github

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPrepareOperatorMessageValidatesAndBindsExactContent(t *testing.T) {
	valid := strings.Repeat("x", OperatorMessageMaxBytes)
	first, err := PrepareOperatorMessage("o/r", 13, 2, valid)
	if err != nil || first.ID == "" {
		t.Fatalf("valid message=%#v err=%v", first, err)
	}
	changed, err := PrepareOperatorMessage("o/r", 13, 2, valid[:len(valid)-1]+"y")
	if err != nil || changed.ID == first.ID {
		t.Fatalf("exact content did not change identity: first=%q changed=%q err=%v", first.ID, changed.ID, err)
	}
	for _, message := range []string{"", " leading", "trailing ", "nul\x00byte", strings.Repeat("x", OperatorMessageMaxBytes+1), string([]byte{0xff})} {
		if _, err := PrepareOperatorMessage("o/r", 13, 2, message); err == nil {
			t.Fatalf("invalid message accepted: %q", message[:min(len(message), 20)])
		}
	}
	if _, err := PrepareOperatorMessage("o/r", 0, 2, "valid"); err == nil {
		t.Fatal("invalid target accepted")
	}
}

func TestOperatorMessageRecordingIsRestartSafeAndDeduplicated(t *testing.T) {
	comments := []map[string]any{}
	posts := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodGet:
			body, _ := json.Marshal(comments)
			return httpResponse(http.StatusOK, string(body), nil), nil
		case http.MethodPost:
			var input struct {
				Body string `json:"body"`
			}
			body, _ := io.ReadAll(request.Body)
			if json.Unmarshal(body, &input) != nil {
				t.Fatal("invalid mutation body")
			}
			posts++
			comments = append(comments, map[string]any{
				"body": input.Body, "created_at": time.Date(2026, 8, 19, 12, posts, 0, 0, time.UTC), "user": map[string]any{"id": 42},
			})
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			return httpResponse(http.StatusMethodNotAllowed, `{}`, nil), nil
		}
	})
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: transport}}
	cfg := PRAdapterConfig{Repository: "o/r", ActorID: 42}
	message, _ := PrepareOperatorMessage("o/r", 13, 2, "Run the focused recovery test, then update the issue.")

	accepted, err := RecordOperatorMessage(t.Context(), api, cfg, message)
	if err != nil || accepted.State != "queued" || posts != 1 {
		t.Fatalf("accepted=%#v posts=%d err=%v", accepted, posts, err)
	}
	if _, err := RecordOperatorMessage(t.Context(), api, cfg, message); err != nil || posts != 1 {
		t.Fatalf("duplicate created another comment: posts=%d err=%v", posts, err)
	}
	claimed, err := RecordOperatorMessageClaim(t.Context(), api, cfg, accepted)
	if err != nil || claimed.State != "claimed" || posts != 2 {
		t.Fatalf("claim=%#v posts=%d err=%v", claimed, posts, err)
	}

	// A new API value models daemon restart; GitHub comments alone recover the claim.
	restarted := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: transport}}
	recovered, err := FetchOperatorMessages(t.Context(), restarted, cfg, 13)
	if err != nil || len(recovered) != 1 || recovered[0].ID != message.ID || recovered[0].State != "claimed" {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	if _, err := RecordOperatorMessageClaim(t.Context(), restarted, cfg, recovered[0]); err != nil || posts != 2 {
		t.Fatalf("duplicate claim posts=%d err=%v", posts, err)
	}
	if err := RecordOperatorMessageOutcome(t.Context(), restarted, cfg, recovered[0], "delivered", ""); err != nil || posts != 3 {
		t.Fatalf("record outcome posts=%d err=%v", posts, err)
	}
	if err := RecordOperatorMessageOutcome(t.Context(), restarted, cfg, recovered[0], "delivered", ""); err != nil || posts != 3 {
		t.Fatalf("duplicate outcome posts=%d err=%v", posts, err)
	}
	final, err := FetchOperatorMessages(t.Context(), restarted, cfg, 13)
	if err != nil || len(final) != 1 || final[0].State != "delivered" || final[0].Message != message.Message {
		t.Fatalf("final=%#v err=%v", final, err)
	}
}
