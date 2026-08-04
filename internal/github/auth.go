package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JWTSource interface {
	JWT(time.Time) (string, error)
}

type TokenSource interface {
	Token(context.Context) (InstallationToken, error)
}

type AppJWT struct {
	AppID string
	Key   *rsa.PrivateKey
}

func (a AppJWT) JWT(now time.Time) (string, error) {
	if a.AppID == "" || a.Key == nil {
		return "", errors.New("GitHub App ID and private key are required")
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": a.AppID})
	unsigned := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, a.Key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign GitHub App JWT: %w", err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

type InstallationToken struct {
	Value     string
	ExpiresAt time.Time
}

type InstallationTokens struct {
	BaseURL        string
	InstallationID int64
	JWTs           JWTSource
	HTTP           *http.Client
	Now            func() time.Time
	RefreshBefore  time.Duration
	mu             sync.Mutex
	cached         InstallationToken
}

func (s *InstallationTokens) Token(ctx context.Context) (InstallationToken, error) {
	if s.InstallationID <= 0 || s.JWTs == nil {
		return InstallationToken{}, errors.New("GitHub installation ID and JWT source are required")
	}
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	margin := s.RefreshBefore
	if margin == 0 {
		margin = time.Minute
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached.Value != "" && s.cached.ExpiresAt.After(now().Add(margin)) {
		return s.cached, nil
	}
	jwt, err := s.JWTs.JWT(now())
	if err != nil {
		return InstallationToken{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.BaseURL, "/")+"/app/installations/"+strconv.FormatInt(s.InstallationID, 10)+"/access_tokens", nil)
	if err != nil {
		return InstallationToken{}, err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	client := s.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return InstallationToken{}, fmt.Errorf("exchange GitHub installation token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return InstallationToken{}, responseError("exchange GitHub installation token", resp)
	}
	var body struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := decodeJSON(resp.Body, &body); err != nil || body.Token == "" || !body.ExpiresAt.After(now()) {
		return InstallationToken{}, errors.New("exchange GitHub installation token: invalid response")
	}
	s.cached = InstallationToken{Value: body.Token, ExpiresAt: body.ExpiresAt}
	return s.cached, nil
}

// ParsePrivateKeyPEM parses a GitHub App private key. GitHub issues PKCS#1
// ("BEGIN RSA PRIVATE KEY"); PKCS#8 ("BEGIN PRIVATE KEY") is also accepted
// defensively since some key managers re-encode to it.
func ParsePrivateKeyPEM(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("GitHub App private key is not valid PEM")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse GitHub App private key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("GitHub App private key must be RSA")
	}
	return key, nil
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
	dec := json.NewDecoder(strings.NewReader(string(b)))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}
