package github

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Hint struct {
	Event, Delivery string
	RepositoryID    int64
	InstallationID  int64
	Issue           int
}

type Webhook struct {
	Secret         []byte
	RepositoryID   int64
	InstallationID int64
	MaxBody        int64
	Hints          chan<- Hint
	Deliveries     *DeliveryCache
}

var webhookEvents = map[string]bool{
	"issues": true, "issue_comment": true, "pull_request": true, "pull_request_review": true,
	"pull_request_review_comment": true, "check_run": true, "check_suite": true, "status": true,
	"push": true, "installation": true, "installation_repositories": true, "repository_ruleset": true,
}

func (h Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if r.Method != http.MethodPost || mediaErr != nil || mediaType != "application/json" {
		http.Error(w, "invalid webhook request", http.StatusUnsupportedMediaType)
		return
	}
	event, delivery := r.Header.Get("X-GitHub-Event"), r.Header.Get("X-GitHub-Delivery")
	if !webhookEvents[event] || delivery == "" || len(delivery) > 128 {
		http.Error(w, "invalid webhook headers", http.StatusBadRequest)
		return
	}
	limit := h.MaxBody
	if limit == 0 {
		limit = 1 << 20
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if !validSignature(h.Secret, body, r.Header.Get("X-Hub-Signature-256")) {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}
	var envelope struct {
		Installation struct {
			ID int64 `json:"id"`
		} `json:"installation"`
		Repository *struct {
			ID int64 `json:"id"`
		} `json:"repository"`
		Issue *struct {
			Number int `json:"number"`
		} `json:"issue"`
		PullRequest *struct {
			Number int `json:"number"`
		} `json:"pull_request"`
	}
	repositoryWide := event == "installation" || event == "installation_repositories"
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Installation.ID != h.InstallationID || envelope.Repository != nil && envelope.Repository.ID != h.RepositoryID || !repositoryWide && envelope.Repository == nil {
		http.Error(w, "webhook target mismatch", http.StatusBadRequest)
		return
	}
	hint := Hint{Event: event, Delivery: delivery, RepositoryID: h.RepositoryID, InstallationID: envelope.Installation.ID}
	if envelope.Issue != nil {
		hint.Issue = envelope.Issue.Number
	} else if envelope.PullRequest != nil {
		hint.Issue = envelope.PullRequest.Number
	}
	offer := func() bool {
		select {
		case h.Hints <- hint:
			return true
		default:
			return false
		}
	}
	if h.Deliveries != nil {
		if duplicate, accepted := h.Deliveries.Offer(delivery, offer); duplicate || accepted {
			w.WriteHeader(http.StatusAccepted)
			return
		}
	} else if offer() {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	http.Error(w, "reconciliation queue unavailable", http.StatusServiceUnavailable)
}

func validSignature(secret, body []byte, value string) bool {
	if len(secret) == 0 || !strings.HasPrefix(value, "sha256=") {
		return false
	}
	want, err := hex.DecodeString(strings.TrimPrefix(value, "sha256="))
	if err != nil || len(want) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), want)
}

type DeliveryCache struct {
	mu    sync.Mutex
	limit int
	list  *list.List
	items map[string]*list.Element
}

func NewDeliveryCache(limit int) *DeliveryCache {
	return &DeliveryCache{limit: max(1, limit), list: list.New(), items: make(map[string]*list.Element)}
}

func (c *DeliveryCache) Offer(id string, offer func() bool) (duplicate, accepted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[id]; element != nil {
		c.list.MoveToFront(element)
		return true, true
	}
	if !offer() {
		return false, false
	}
	c.items[id] = c.list.PushFront(id)
	if c.list.Len() > c.limit {
		old := c.list.Back()
		delete(c.items, old.Value.(string))
		c.list.Remove(old)
	}
	return false, true
}

type Reconciler struct {
	Hints    chan Hint
	FullRead func() error
}

func (r Reconciler) RunOnce() error {
	if r.FullRead == nil {
		return errors.New("reconciliation read is required")
	}
	return r.FullRead()
}

func (r Reconciler) Run(ctx context.Context, interval time.Duration, failures chan<- error) {
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	reconcile := func() {
		if err := r.RunOnce(); err != nil && failures != nil {
			select {
			case failures <- err:
			default:
			}
		}
	}
	reconcile()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		case <-r.Hints:
			reconcile()
		}
	}
}

func SignWebhook(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return fmt.Sprintf("sha256=%x", mac.Sum(nil))
}
