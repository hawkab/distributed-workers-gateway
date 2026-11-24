package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWaitingClaimWakesAfterCommit(t *testing.T) {
	s := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunNotifications(ctx)
	register(t, s, "waiting")
	got := make(chan *ClaimedJob, 1)
	err := make(chan error, 1)
	go func() {
		j, e := s.claimWait(ctx, "waiting", []string{"sha256"}, nil, 20*time.Second)
		if e != nil {
			err <- e
		} else {
			got <- j
		}
	}()
	id := enqueue(t, s, "sha256", "wake-on-commit")
	select {
	case j := <-got:
		if j.ID != id {
			t.Fatal("wrong claimed job")
		}
	case e := <-err:
		t.Fatal(e)
	case <-time.After(3 * time.Second):
		t.Fatal("enqueue did not wake claimant")
	}
}
func TestWaitingClaimCancellation(t *testing.T) {
	s := fixture(t)
	register(t, s, "idle")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := s.claimWait(ctx, "idle", nil, nil, 20*time.Second)
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestMinimalReadyResponseDoesNotAggregateLargeResults(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	ids := []string{}
	text := strings.Repeat("x", 9<<20)
	raw, _ := json.Marshal(map[string]string{"text": text})
	for _, worker := range []string{"one", "two"} {
		register(t, s, worker)
		id := enqueue(t, s, "sha256", worker)
		j, e := s.Claim(ctx, worker, []string{"sha256"})
		must(t, e)
		c := done(j, worker)
		c.Result = raw
		_, e = s.Complete(ctx, id, c)
		must(t, e)
		ids = append(ids, id)
	}
	h := (&API{Store: s, AdminToken: "operator"}).Handler()
	body, _ := json.Marshal(map[string]any{"ids": ids})
	req := httptest.NewRequest("POST", "/v1/admin/jobs/wait", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer operator")
	req.Header.Set("Prefer", "wait=20, return=minimal")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || w.Body.Len() > 1024 {
		t.Fatal("ready response carried aggregate payload", w.Code, w.Body.Len())
	}
	var result struct {
		Jobs []struct {
			ID     string          `json:"id"`
			Ref    string          `json:"result_ref"`
			Result json.RawMessage `json:"result"`
		} `json:"jobs"`
	}
	must(t, json.Unmarshal(w.Body.Bytes(), &result))
	if len(result.Jobs) != 2 {
		t.Fatal("missing summaries")
	}
	for _, row := range result.Jobs {
		if row.Ref == "" || len(row.Result) > 0 {
			t.Fatal("not a result reference")
		}
		req = httptest.NewRequest("GET", row.Ref, nil)
		req.Header.Set("Authorization", "Bearer operator")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		var value struct {
			Result struct {
				Result struct {
					Text string `json:"text"`
				} `json:"result"`
			} `json:"result"`
		}
		must(t, json.Unmarshal(w.Body.Bytes(), &value))
		if value.Result.Result.Text != text {
			t.Fatal("result truncated")
		}
	}
}
