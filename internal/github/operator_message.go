package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	OperatorMessageMaxBytes     = 8 << 10
	operatorMessageMarkerPrefix = "<!-- agent-symphony:operator-message:v1\n"
	operatorClaimMarkerPrefix   = "<!-- agent-symphony:operator-message-claim:v1\n"
	operatorOutcomeMarkerPrefix = "<!-- agent-symphony:operator-message-outcome:v1\n"
)

type OperatorMessage struct {
	Version    int       `json:"version"`
	Repository string    `json:"repository"`
	Issue      int       `json:"issue"`
	Attempt    int       `json:"attempt"`
	ID         string    `json:"id"`
	Message    string    `json:"message"`
	State      string    `json:"state,omitempty"`
	Diagnostic string    `json:"diagnostic,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
}

type operatorMessagePayload struct {
	Version    int    `json:"version"`
	Repository string `json:"repository"`
	Issue      int    `json:"issue"`
	Attempt    int    `json:"attempt"`
	ID         string `json:"id"`
	Message    string `json:"message"`
}

type operatorMessageOutcome struct {
	Version    int    `json:"version"`
	Issue      int    `json:"issue"`
	Attempt    int    `json:"attempt"`
	ID         string `json:"id"`
	State      string `json:"state"`
	Diagnostic string `json:"diagnostic,omitempty"`
}

type operatorMessageClaim struct {
	Version int    `json:"version"`
	Issue   int    `json:"issue"`
	Attempt int    `json:"attempt"`
	ID      string `json:"id"`
}

func PrepareOperatorMessage(repository string, issue, attempt int, message string) (OperatorMessage, error) {
	if repository == "" || issue < 1 || attempt < 1 || message != strings.TrimSpace(message) || message == "" || len(message) > OperatorMessageMaxBytes || !utf8.ValidString(message) {
		return OperatorMessage{}, errors.New("operator message requires an exact repository, positive issue and attempt, and 1-8192 bytes of UTF-8 text without surrounding whitespace")
	}
	for _, r := range message {
		if r == 0 || r < 0x20 && r != '\n' && r != '\t' {
			return OperatorMessage{}, errors.New("operator message contains unsupported control characters")
		}
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("operator-message-v1\x00%s\x00%d\x00%d\x00%s", repository, issue, attempt, message)))
	return OperatorMessage{Version: 1, Repository: repository, Issue: issue, Attempt: attempt, ID: hex.EncodeToString(sum[:]), Message: message, State: "queued"}, nil
}

func operatorMessageMarker(message OperatorMessage) (string, error) {
	want, err := PrepareOperatorMessage(message.Repository, message.Issue, message.Attempt, message.Message)
	if err != nil || message.Version != 1 || message.ID != want.ID {
		return "", errors.New("invalid operator message marker")
	}
	b, _ := json.Marshal(operatorMessagePayload{want.Version, want.Repository, want.Issue, want.Attempt, want.ID, want.Message})
	return operatorMessageMarkerPrefix + string(b) + "\n-->", nil
}

func parseOperatorMessageMarker(body string) (OperatorMessage, error) {
	var payload operatorMessagePayload
	if err := parseBoundedMarker(body, operatorMessageMarkerPrefix, &payload); err != nil {
		return OperatorMessage{}, err
	}
	message := OperatorMessage{Version: payload.Version, Repository: payload.Repository, Issue: payload.Issue, Attempt: payload.Attempt, ID: payload.ID, Message: payload.Message, State: "queued"}
	want, err := PrepareOperatorMessage(message.Repository, message.Issue, message.Attempt, message.Message)
	if err != nil || message.Version != 1 || message.ID != want.ID {
		return OperatorMessage{}, errors.New("operator message marker is invalid")
	}
	return message, nil
}

func parseOperatorOutcomeMarker(body string) (operatorMessageOutcome, error) {
	var outcome operatorMessageOutcome
	if err := parseBoundedMarker(body, operatorOutcomeMarkerPrefix, &outcome); err != nil {
		return outcome, err
	}
	if outcome.Version != 1 || outcome.Issue < 1 || outcome.Attempt < 1 || len(outcome.ID) != 64 || !regexpSHA.MatchString(outcome.ID) || !validOperatorOutcome(outcome.State) || len(outcome.Diagnostic) > 256 {
		return operatorMessageOutcome{}, errors.New("operator message outcome marker is invalid")
	}
	return outcome, nil
}

func parseOperatorClaimMarker(body string) (operatorMessageClaim, error) {
	var claim operatorMessageClaim
	if err := parseBoundedMarker(body, operatorClaimMarkerPrefix, &claim); err != nil {
		return claim, err
	}
	if claim.Version != 1 || claim.Issue < 1 || claim.Attempt < 1 || len(claim.ID) != 64 || !regexpSHA.MatchString(claim.ID) {
		return operatorMessageClaim{}, errors.New("operator message claim marker is invalid")
	}
	return claim, nil
}

func parseBoundedMarker(body, prefix string, dst any) error {
	start := strings.Index(body, prefix)
	if start < 0 || len(body) > 64<<10 || strings.Count(body, prefix) != 1 {
		return errors.New("operator message marker is missing or duplicated")
	}
	rest := body[start+len(prefix):]
	end := strings.Index(rest, "\n-->")
	if end < 0 || strings.TrimSpace(rest[end+4:]) != "" {
		return errors.New("operator message marker is malformed")
	}
	decoder := json.NewDecoder(strings.NewReader(rest[:end]))
	decoder.DisallowUnknownFields()
	if decoder.Decode(dst) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("operator message marker is invalid")
	}
	return nil
}

func FetchOperatorMessages(ctx context.Context, api API, cfg PRAdapterConfig, issue int) ([]OperatorMessage, error) {
	if cfg.Repository == "" || cfg.ActorID < 1 || issue < 1 {
		return nil, errors.New("operator message lookup requires repository, actor, and issue")
	}
	messages := map[string]OperatorMessage{}
	claims := map[string]operatorMessageClaim{}
	claimTimes := map[string]time.Time{}
	outcomes := map[string]operatorMessageOutcome{}
	outcomeTimes := map[string]time.Time{}
	for page := 1; page <= recoveryPageLimit; page++ {
		var comments []struct {
			Body      string           `json:"body"`
			CreatedAt time.Time        `json:"created_at"`
			User      struct{ ID int } `json:"user"`
		}
		if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", cfg.Repository, issue, page), "", &comments); err != nil {
			return nil, err
		}
		for _, comment := range comments {
			if comment.User.ID != cfg.ActorID {
				continue
			}
			if strings.Contains(comment.Body, operatorMessageMarkerPrefix) {
				message, err := parseOperatorMessageMarker(comment.Body)
				if err != nil || message.Repository != cfg.Repository || message.Issue != issue {
					return nil, errors.New("trusted operator message marker is invalid")
				}
				message.UpdatedAt = comment.CreatedAt.UTC()
				if prior, ok := messages[message.ID]; ok && prior.Message != message.Message {
					return nil, errors.New("operator message identity is contradictory")
				}
				messages[message.ID] = message
			}
			if strings.Contains(comment.Body, operatorOutcomeMarkerPrefix) {
				outcome, err := parseOperatorOutcomeMarker(comment.Body)
				if err != nil || outcome.Issue != issue {
					return nil, errors.New("trusted operator message outcome marker is invalid")
				}
				if prior, ok := outcomes[outcome.ID]; ok && prior != outcome {
					return nil, errors.New("operator message outcome is contradictory")
				}
				outcomes[outcome.ID] = outcome
				outcomeTimes[outcome.ID] = comment.CreatedAt.UTC()
			}
			if strings.Contains(comment.Body, operatorClaimMarkerPrefix) {
				claim, err := parseOperatorClaimMarker(comment.Body)
				if err != nil || claim.Issue != issue {
					return nil, errors.New("trusted operator message claim marker is invalid")
				}
				if prior, ok := claims[claim.ID]; ok && prior != claim {
					return nil, errors.New("operator message claim is contradictory")
				}
				claims[claim.ID] = claim
				claimTimes[claim.ID] = comment.CreatedAt.UTC()
			}
		}
		if len(comments) < 100 {
			result := make([]OperatorMessage, 0, len(messages))
			for _, message := range messages {
				if claim, ok := claims[message.ID]; ok && claim.Attempt == message.Attempt {
					message.State, message.UpdatedAt = "claimed", claimTimes[message.ID]
				}
				if outcome, ok := outcomes[message.ID]; ok && outcome.Attempt == message.Attempt {
					message.State, message.Diagnostic = outcome.State, outcome.Diagnostic
					message.UpdatedAt = outcomeTimes[message.ID]
				}
				result = append(result, message)
			}
			slicesSortOperatorMessages(result)
			return result, nil
		}
	}
	return nil, errors.New("issue comments exceed bounded recovery limit")
}

func RecordOperatorMessageClaim(ctx context.Context, api API, cfg PRAdapterConfig, message OperatorMessage) (OperatorMessage, error) {
	messages, err := FetchOperatorMessages(ctx, api, cfg, message.Issue)
	if err != nil {
		return OperatorMessage{}, err
	}
	for _, existing := range messages {
		if existing.ID != message.ID {
			continue
		}
		if existing.Attempt != message.Attempt || existing.Message != message.Message {
			return OperatorMessage{}, errors.New("operator message claim identity changed")
		}
		if existing.State == "claimed" {
			return existing, nil
		}
		if existing.State != "queued" {
			return OperatorMessage{}, errors.New("operator message already has a terminal outcome")
		}
		claim := operatorMessageClaim{Version: 1, Issue: message.Issue, Attempt: message.Attempt, ID: message.ID}
		b, _ := json.Marshal(claim)
		marker := operatorClaimMarkerPrefix + string(b) + "\n-->"
		body, _ := AttributedBody(message.Issue, message.Attempt, "Operator message delivery claimed for the exact active attempt.")
		createErr := api.CreateIssueComment(ctx, cfg.Repository, message.Issue, body+"\n\n"+marker, Mutation{Issue: message.Issue, Attempt: message.Attempt})
		confirmed, confirmErr := FetchOperatorMessages(ctx, api, cfg, message.Issue)
		if confirmErr != nil {
			return OperatorMessage{}, confirmErr
		}
		for _, candidate := range confirmed {
			if candidate.ID == message.ID && candidate.State == "claimed" {
				return candidate, nil
			}
		}
		if createErr != nil {
			return OperatorMessage{}, createErr
		}
		return OperatorMessage{}, errors.New("operator message claim was not observable")
	}
	return OperatorMessage{}, errors.New("operator message is not durably accepted")
}

func slicesSortOperatorMessages(messages []OperatorMessage) {
	slices.SortFunc(messages, func(a, b OperatorMessage) int {
		if compared := a.UpdatedAt.Compare(b.UpdatedAt); compared != 0 {
			return compared
		}
		return strings.Compare(a.ID, b.ID)
	})
}

func RecordOperatorMessage(ctx context.Context, api API, cfg PRAdapterConfig, message OperatorMessage) (OperatorMessage, error) {
	want, err := PrepareOperatorMessage(message.Repository, message.Issue, message.Attempt, message.Message)
	if err != nil || message.ID != want.ID || cfg.Repository != message.Repository || cfg.ActorID < 1 {
		return OperatorMessage{}, errors.New("invalid operator message recording request")
	}
	find := func() (OperatorMessage, bool, error) {
		messages, err := FetchOperatorMessages(ctx, api, cfg, message.Issue)
		if err != nil {
			return OperatorMessage{}, false, err
		}
		for _, candidate := range messages {
			if candidate.ID == message.ID {
				return candidate, true, nil
			}
		}
		return OperatorMessage{}, false, nil
	}
	if existing, ok, err := find(); err != nil || ok {
		return existing, err
	}
	marker, _ := operatorMessageMarker(message)
	body, _ := AttributedBody(message.Issue, message.Attempt, "Operator message accepted and queued for the exact active attempt.")
	createErr := api.CreateIssueComment(ctx, cfg.Repository, message.Issue, body+"\n\n"+marker, Mutation{Issue: message.Issue, Attempt: message.Attempt})
	if existing, ok, err := find(); err != nil {
		return OperatorMessage{}, err
	} else if ok {
		return existing, nil
	}
	if createErr != nil {
		return OperatorMessage{}, createErr
	}
	return OperatorMessage{}, errors.New("operator message creation was not observable")
}

func RecordOperatorMessageOutcome(ctx context.Context, api API, cfg PRAdapterConfig, message OperatorMessage, state, diagnostic string) error {
	if !validOperatorOutcome(state) || len(diagnostic) > 256 {
		return errors.New("invalid operator message outcome")
	}
	messages, err := FetchOperatorMessages(ctx, api, cfg, message.Issue)
	if err != nil {
		return err
	}
	for _, existing := range messages {
		if existing.ID != message.ID {
			continue
		}
		if existing.Attempt != message.Attempt || existing.Message != message.Message {
			return errors.New("operator message outcome identity changed")
		}
		if existing.State != "queued" && existing.State != "claimed" {
			if existing.State == state && existing.Diagnostic == diagnostic {
				return nil
			}
			return errors.New("operator message already has a different outcome")
		}
		outcome := operatorMessageOutcome{Version: 1, Issue: message.Issue, Attempt: message.Attempt, ID: message.ID, State: state, Diagnostic: diagnostic}
		b, _ := json.Marshal(outcome)
		marker := operatorOutcomeMarkerPrefix + string(b) + "\n-->"
		body, _ := AttributedBody(message.Issue, message.Attempt, "Operator message outcome: "+state+".")
		createErr := api.CreateIssueComment(ctx, cfg.Repository, message.Issue, body+"\n\n"+marker, Mutation{Issue: message.Issue, Attempt: message.Attempt})
		confirmed, confirmErr := FetchOperatorMessages(ctx, api, cfg, message.Issue)
		if confirmErr != nil {
			return confirmErr
		}
		for _, candidate := range confirmed {
			if candidate.ID == message.ID && candidate.State == state && candidate.Diagnostic == diagnostic {
				return nil
			}
		}
		if createErr != nil {
			return createErr
		}
		return errors.New("operator message outcome was not observable")
	}
	return errors.New("operator message is not durably accepted")
}

func validOperatorOutcome(state string) bool {
	return state == "delivered" || state == "rejected" || state == "failed"
}
