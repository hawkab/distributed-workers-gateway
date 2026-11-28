package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestWithdrawLostAcknowledgment(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	job := enqueue(t, s, "sha256", "withdraw")
	other := enqueue(t, s, "sha256", "other")
	op := newID()
	route := (&API{Store: s, AdminToken: "operator"}).Handler()
	var drop atomic.Bool
	drop.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !drop.Swap(false) {
			route.ServeHTTP(w, r)
			return
		}
		// Discard the response only after PostgreSQL commits. The client's actual
		// HTTP connection then ends without a status line or any receipt bytes.
		rec := httptest.NewRecorder()
		route.ServeHTTP(rec, r)
		if rec.Code != 200 {
			t.Errorf("initial withdrawal got %d", rec.Code)
		}
		c, _, e := w.(http.Hijacker).Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		c.Close()
	}))
	defer server.Close()
	post := func(id, key, token string) (int, WithdrawalReceipt, error) {
		x, _ := json.Marshal(map[string]string{"operation_id": key})
		req, e := http.NewRequest(http.MethodPost, server.URL+"/v1/admin/jobs/"+id+"/withdraw-unclaimed", bytes.NewReader(x))
		if e != nil {
			return 0, WithdrawalReceipt{}, e
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, e := server.Client().Do(req)
		if e != nil {
			return 0, WithdrawalReceipt{}, e
		}
		defer resp.Body.Close()
		var got WithdrawalReceipt
		if resp.StatusCode == 200 {
			e = json.NewDecoder(resp.Body).Decode(&got)
		}
		return resp.StatusCode, got, e
	}
	if _, _, e := post(job, op, "operator"); e == nil {
		t.Fatal("lost ACK reached client")
	}
	want := WithdrawalReceipt{JobID: job, OperationID: op, Withdrawn: true}
	for i := 0; i < 2; i++ {
		status, got, e := post(job, op, "operator")
		if e != nil || status != 200 || got != want {
			t.Fatal(status, got, e)
		}
	}
	for _, tc := range []struct {
		id, key, token string
		status         int
	}{{job, newID(), "operator", 409}, {other, op, "operator", 409}, {newID(), newID(), "operator", 404}, {other, "invalid", "operator", 400}, {other, newID(), "wrong", 401}} {
		status, _, e := post(tc.id, tc.key, tc.token)
		if e != nil || status != tc.status {
			t.Fatal(status, e, tc.status)
		}
	}
	var cancelled int
	must(t, s.Pool.QueryRow(ctx, "SELECT count(*) FROM "+s.table("outbox")+" WHERE event_type='job.cancelled'").Scan(&cancelled))
	if count(t, s, "unclaimed_withdrawals") != 1 || cancelled != 1 {
		t.Fatal("replayed a state transition")
	}
	register(t, s, "claimant")
	j, e := s.Claim(ctx, "claimant", []string{"sha256"})
	must(t, e)
	if j.ID != other {
		t.Fatal("withdrew the wrong job")
	}
	if _, e = s.WithdrawUnclaimed(ctx, other, newID()); !errors.Is(e, ErrConflict) {
		t.Fatal("withdrew a live lease", e)
	}
	_, e = s.Pool.Exec(ctx, "UPDATE "+s.table("jobs")+" SET status='queued',active_attempt_id=NULL,fencing_token=0 WHERE id=$1", other)
	must(t, e)
	if _, e = s.WithdrawUnclaimed(ctx, other, newID()); !errors.Is(e, ErrConflict) {
		t.Fatal("ignored historical attempt", e)
	}
	ordinary := enqueue(t, s, "sha256", "cancelled-elsewhere")
	must(t, s.Cancel(ctx, ordinary))
	if _, e = s.WithdrawUnclaimed(ctx, ordinary, newID()); !errors.Is(e, ErrConflict) {
		t.Fatal("attributed ordinary cancellation to CAS", e)
	}
}
func TestConcurrentClaimOrWithdraw(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	for i := 0; i < 24; i++ {
		worker := fmt.Sprintf("race-%d", i)
		register(t, s, worker)
		id, e := s.Enqueue(ctx, EnqueueJob{Operation: "sha256", Input: json.RawMessage(`{"text":"hello"}`), IdempotencyKey: worker, RequiredCapabilities: []string{worker}})
		must(t, e)
		start := make(chan struct{})
		claims := make(chan error, 1)
		withdrawals := make(chan error, 1)
		op := newID()
		go func() {
			<-start
			j, e := s.Claim(ctx, worker, []string{"sha256", worker})
			if e == nil && j.ID != id {
				e = fmt.Errorf("wrong claimed job")
			}
			claims <- e
		}()
		go func() { <-start; _, e := s.WithdrawUnclaimed(ctx, id, op); withdrawals <- e }()
		close(start)
		c, x := <-claims, <-withdrawals
		var attempts, receipts, fence int
		var state string
		must(t, s.Pool.QueryRow(ctx, "SELECT j.status,j.fencing_token,(SELECT count(*) FROM "+s.table("attempts")+" WHERE job_id=j.id),(SELECT count(*) FROM "+s.table("unclaimed_withdrawals")+" WHERE job_id=j.id) FROM "+s.table("jobs")+" j WHERE j.id=$1", id).Scan(&state, &fence, &attempts, &receipts))
		if x == nil {
			if !errors.Is(c, ErrNotFound) || state != "cancelled" || fence != 0 || attempts != 0 || receipts != 1 {
				t.Fatal("withdraw/claim overlap", c, state, fence, attempts, receipts)
			}
		} else if !errors.Is(x, ErrConflict) || c != nil || state != "running" || fence != 1 || attempts != 1 || receipts != 0 {
			t.Fatal("claim/withdraw overlap", x, c, state, fence, attempts, receipts)
		}
	}
}

func TestConcurrentWithdrawalOperationOwnership(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	first, second := enqueue(t, s, "sha256", "first"), enqueue(t, s, "sha256", "second")
	operation := newID()
	start := make(chan struct{})
	answers := make(chan WithdrawalReceipt, 2)
	failures := make(chan error, 2)
	for _, job := range []string{first, second} {
		go func(id string) {
			<-start
			r, err := s.WithdrawUnclaimed(ctx, id, operation)
			if err != nil {
				failures <- err
			} else {
				answers <- r
			}
		}(job)
	}
	close(start)
	var won WithdrawalReceipt
	var conflicts int
	for i := 0; i < 2; i++ {
		select {
		case r := <-answers:
			won = r
		case e := <-failures:
			if !errors.Is(e, ErrConflict) {
				t.Fatal(e)
			}
			conflicts++
		}
	}
	if won.OperationID != operation || conflicts != 1 {
		t.Fatal("operation was not exclusively bound")
	}
	var other string
	if won.JobID == first {
		other = second
	} else {
		other = first
	}
	newOp := newID()
	result, err := s.WithdrawUnclaimed(ctx, other, newOp)
	must(t, err)
	if result.JobID != other || result.OperationID != newOp {
		t.Fatal("losing job was mutated")
	}
	start = make(chan struct{})
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(index int) {
			<-start
			op := operation
			if index%2 != 0 {
				op = newID()
			}
			r, e := s.WithdrawUnclaimed(ctx, won.JobID, op)
			if index%2 == 0 && r == won && e == nil || index%2 != 0 && errors.Is(e, ErrConflict) {
				errs <- nil
			} else {
				errs <- fmt.Errorf("unexpected replay %v, %v", r, e)
			}
		}(i)
	}
	close(start)
	for i := 0; i < 8; i++ {
		must(t, <-errs)
	}
	if count(t, s, "unclaimed_withdrawals") != 2 {
		t.Fatal("unexpected persisted claims")
	}
}
