package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type API struct {
	BaseURL string
	Tokens  TokenSource
	HTTP    *http.Client
	Sleep   func(context.Context, time.Duration) error
	Retries int
}

type Mutation struct {
	Issue   int
	Attempt int
}

// VerifyInstallation binds the supplied installation token to the configured App.
func (a API) VerifyInstallation(ctx context.Context, appID int64) error {
	var installation struct {
		AppID int64 `json:"app_id"`
	}
	if _, _, err := a.Read(ctx, "/installation", "", &installation); err != nil {
		return fmt.Errorf("verify GitHub App installation: %w", err)
	}
	if appID <= 0 || installation.AppID != appID {
		return errors.New("GitHub installation token does not belong to the configured App")
	}
	return nil
}

// RepositoryID fetches the numeric GitHub repository ID for "owner/repo",
// used to scope webhook delivery validation to the exact configured repository.
func (a API) RepositoryID(ctx context.Context, repository string) (int64, error) {
	var repo struct {
		ID int64 `json:"id"`
	}
	if _, _, err := a.Read(ctx, "/repos/"+repository, "", &repo); err != nil {
		return 0, fmt.Errorf("fetch GitHub repository ID: %w", err)
	}
	if repo.ID <= 0 {
		return 0, errors.New("GitHub repository ID is invalid")
	}
	return repo.ID, nil
}

type ambiguousMutationError struct{ error }

type responseStatusError struct {
	operation string
	status    int
	message   string
}

func (e *responseStatusError) Error() string {
	return fmt.Sprintf("%s: %s", e.operation, e.message)
}

func isResponseStatus(err error, status int) bool {
	var response *responseStatusError
	return errors.As(err, &response) && response.status == status
}

func IsAmbiguousMutation(err error) bool {
	var ambiguous *ambiguousMutationError
	return errors.As(err, &ambiguous)
}

func (a API) Read(ctx context.Context, path, etag string, dst any) (string, bool, error) {
	if !strings.HasPrefix(path, "/") {
		return "", false, errors.New("GitHub API path must start with /")
	}
	retries := a.Retries
	if retries == 0 {
		retries = 3
	}
	for attempt := 0; ; attempt++ {
		resp, err := a.do(ctx, http.MethodGet, path, etag, nil, Mutation{})
		if err == nil && resp.StatusCode == http.StatusNotModified {
			resp.Body.Close()
			return resp.Header.Get("ETag"), false, nil
		}
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			defer resp.Body.Close()
			if err := decodeJSON(resp.Body, dst); err != nil {
				return "", false, fmt.Errorf("decode GitHub read: %w", err)
			}
			return resp.Header.Get("ETag"), true, nil
		}
		if err == nil && !transient(resp) {
			defer resp.Body.Close()
			return "", false, responseError("GitHub read", resp)
		}
		if attempt >= retries {
			if err != nil {
				return "", false, fmt.Errorf("GitHub read: %w", err)
			}
			defer resp.Body.Close()
			return "", false, responseError("GitHub read after retries", resp)
		}
		delay := retryDelay(resp, attempt)
		if resp != nil {
			resp.Body.Close()
		}
		if err := a.sleep(ctx, delay); err != nil {
			return "", false, err
		}
	}
}

func (a API) readGraphQL(ctx context.Context, payload, dst any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := a.do(ctx, http.MethodPost, "/graphql", "", b, Mutation{})
	if err != nil {
		return fmt.Errorf("GitHub GraphQL read: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError("GitHub GraphQL read", resp)
	}
	return decodeJSON(resp.Body, dst)
}

func (a API) Mutate(ctx context.Context, method, path string, body any, attribution Mutation, dst any) error {
	if attribution.Issue <= 0 || attribution.Attempt <= 0 {
		return errors.New("GitHub mutation requires issue and attempt attribution")
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	marker := fmt.Sprintf("<!-- agent-symphony:issue:%d:attempt:%d -->", attribution.Issue, attribution.Attempt)
	var object map[string]json.RawMessage
	var persistedBody string
	if json.Unmarshal(b, &object) != nil || json.Unmarshal(object["body"], &persistedBody) != nil || !strings.Contains(persistedBody, marker) {
		return errors.New("GitHub mutation body must persist issue and attempt attribution")
	}
	resp, err := a.do(ctx, method, path, "", b, attribution)
	if err != nil {
		return &ambiguousMutationError{fmt.Errorf("GitHub mutation outcome is ambiguous; reconcile issue #%d attempt %d: %w", attribution.Issue, attribution.Attempt, err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return responseError("GitHub mutation", resp)
	}
	if dst != nil && resp.StatusCode != http.StatusNoContent {
		if err := decodeJSON(resp.Body, dst); err != nil {
			return &ambiguousMutationError{fmt.Errorf("GitHub mutation outcome is ambiguous; reconcile issue #%d attempt %d: decode response: %w", attribution.Issue, attribution.Attempt, err)}
		}
	}
	return nil
}

func (a API) do(ctx context.Context, method, path, etag string, body []byte, attribution Mutation) (*http.Response, error) {
	if a.Tokens == nil {
		return nil, errors.New("GitHub installation token source is required")
	}
	token, err := a.Tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token.Value)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if attribution.Issue > 0 {
		req.Header.Set("X-Agent-Symphony-Issue", strconv.Itoa(attribution.Issue))
		req.Header.Set("X-Agent-Symphony-Attempt", strconv.Itoa(attribution.Attempt))
	}
	client := a.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

func (a API) sleep(ctx context.Context, d time.Duration) error {
	if a.Sleep != nil {
		return a.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func transient(resp *http.Response) bool {
	return resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 || resp.StatusCode == http.StatusForbidden && (resp.Header.Get("Retry-After") != "" || resp.Header.Get("X-RateLimit-Remaining") == "0"))
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if resp != nil {
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			return min(time.Duration(seconds)*time.Second, time.Minute)
		}
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			if reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
				return min(max(time.Until(time.Unix(reset, 0)), 0), time.Minute)
			}
		}
	}
	base := min(time.Second<<attempt, 30*time.Second)
	return base + time.Duration(rand.Int64N(int64(base/4)+1))
}

func responseError(operation string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return &responseStatusError{operation: operation, status: resp.StatusCode, message: fmt.Sprintf("%s: %s", resp.Status, Redact(string(body)))}
}
