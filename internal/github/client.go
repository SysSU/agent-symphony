package github

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type API struct {
	BaseURL string
	HTTP    *http.Client
	Sleep   func(context.Context, time.Duration) error
	Retries int
}

type Mutation struct {
	Issue   int
	Attempt int
}

type AuthenticatedUser struct {
	ID    int
	Login string
}

func (a API) AuthenticatedUser(ctx context.Context) (AuthenticatedUser, error) {
	var user AuthenticatedUser
	if _, _, err := a.Read(ctx, "/user", "", &user); err != nil {
		return user, fmt.Errorf("authenticate GitHub CLI: %w", err)
	}
	if user.ID <= 0 || strings.TrimSpace(user.Login) == "" {
		return user, errors.New("GitHub CLI returned an invalid authenticated user")
	}
	return user, nil
}

func (a API) VerifyRepository(ctx context.Context, repository string) error {
	if repository == "" {
		return errors.New("GitHub repository is required")
	}
	var repo struct {
		FullName    string `json:"full_name"`
		Permissions struct{ Pull bool }
	}
	if _, _, err := a.Read(ctx, "/repos/"+repository, "", &repo); err != nil {
		return fmt.Errorf("verify GitHub CLI repository access: %w", err)
	}
	if !strings.EqualFold(repo.FullName, repository) || !repo.Permissions.Pull {
		return errors.New("authenticated GitHub CLI account cannot access the configured repository")
	}
	return nil
}

type CLITransport struct{ Path string }

func (t CLITransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path := t.Path
	if path == "" {
		path = "gh"
	}
	endpoint := req.URL.RequestURI()
	if req.URL.Path == "/graphql" {
		endpoint = "graphql"
	}
	args := []string{"api", "--include", "--method", req.Method, endpoint}
	for name, values := range req.Header {
		for _, value := range values {
			args = append(args, "--header", name+":"+value)
		}
	}
	if req.Body != nil {
		args = append(args, "--input", "-")
	}
	cmd := exec.CommandContext(req.Context(), path, args...)
	cmd.Stdin = req.Body
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, runErr := cmd.Output()
	resp, parseErr := http.ReadResponse(bufio.NewReader(bytes.NewReader(stdout)), req)
	if parseErr == nil {
		return resp, nil
	}
	if runErr != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = runErr.Error()
		}
		return nil, fmt.Errorf("GitHub CLI API: %s", Redact(detail))
	}
	return nil, fmt.Errorf("parse GitHub CLI response: %w", parseErr)
}

type ambiguousMutationError struct{ error }

type responseStatusError struct {
	operation, message, githubMessage, documentationURL string
	status                                              int
	structured                                          bool
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
	var input io.Reader
	if body != nil {
		input = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(a.BaseURL, "/")+path, input)
	if err != nil {
		return nil, err
	}
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
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
	var details struct {
		Message          string `json:"message"`
		DocumentationURL string `json:"documentation_url"`
	}
	structured := readErr == nil && len(body) <= 4096 && json.Unmarshal(body, &details) == nil
	body = body[:min(len(body), 4096)]
	return &responseStatusError{operation: operation, status: resp.StatusCode, message: fmt.Sprintf("%s: %s", resp.Status, Redact(string(body))), githubMessage: details.Message, documentationURL: details.DocumentationURL, structured: structured}
}

func decodeJSON(r io.Reader, dst any) error {
	const maxJSONBody = 1 << 20
	b, err := io.ReadAll(io.LimitReader(r, maxJSONBody+1))
	if err != nil {
		return err
	}
	if len(b) > maxJSONBody {
		return errors.New("JSON body exceeds 1 MiB limit")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}
