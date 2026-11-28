package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Notifications are commit-ordered hints. PostgreSQL remains the durable queue;
// reconnects always wake readers to recheck state, including missed events.
func (s *Store) notificationChannel() string {
	h := sha256.Sum256([]byte(s.Schema))
	return "fleet_" + hex.EncodeToString(h[:16])
}
func (s *Store) changes() <-chan struct{} {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

type claimWatch struct {
	changed chan struct{}
	readers int
}

func (s *Store) watchClaims(worker string) *claimWatch {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	if s.claimWaiters == nil {
		s.claimWaiters = make(map[string]*claimWatch)
	}
	watch := s.claimWaiters[worker]
	if watch == nil {
		watch = &claimWatch{changed: make(chan struct{})}
		s.claimWaiters[worker] = watch
	}
	watch.readers++
	return watch
}
func (s *Store) unwatchClaims(worker string, watch *claimWatch) {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	watch.readers--
	if watch.readers == 0 {
		delete(s.claimWaiters, worker)
	}
	if len(s.claimWaiters) == 0 {
		s.claimWaiters = nil
	}
}
func (s *Store) claimChanges(watch *claimWatch) <-chan struct{} {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	return watch.changed
}
func signalClaim(watch *claimWatch) { close(watch.changed); watch.changed = make(chan struct{}) }
func (s *Store) notifyWorker(worker string) {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	if watch := s.claimWaiters[worker]; watch != nil {
		signalClaim(watch)
	}
}
func (s *Store) claimNotificationChannel() string { return s.notificationChannel() + "_claims" }
func claimEvent(kind string) bool                 { return kind == "job.queued" }
func resultEvent(kind string) bool {
	// Complete emits the actual terminal status. Cancelling a queued job makes
	// it terminal immediately, although its event is job.cancel_requested.
	return kind == "job.succeeded" || kind == "job.failed" || kind == "job.cancelled" || kind == "job.cancel_requested"
}
func closeChange(ch *chan struct{}) {
	if *ch != nil {
		close(*ch)
		*ch = nil
	}
}
func (s *Store) notify() {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	closeChange(&s.changed)
	for _, watch := range s.claimWaiters {
		signalClaim(watch)
	}
}
func (s *Store) notifyEvent(kind string) {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	if resultEvent(kind) {
		closeChange(&s.changed)
	}
	if claimEvent(kind) {
		for _, watch := range s.claimWaiters {
			signalClaim(watch)
		}
	}
}
func (s *Store) RunNotifications(ctx context.Context) {
	for ctx.Err() == nil {
		conn, e := pgx.ConnectConfig(ctx, s.Pool.Config().ConnConfig.Copy())
		if e == nil {
			_, e = conn.Exec(ctx, "LISTEN "+pgx.Identifier{s.notificationChannel()}.Sanitize()+"; LISTEN "+pgx.Identifier{s.claimNotificationChannel()}.Sanitize())
		}
		s.notify()
		if e == nil {
			for ctx.Err() == nil {
				notification, err := conn.WaitForNotification(ctx)
				if e = err; e != nil {
					break
				}
				if notification.Channel == s.claimNotificationChannel() {
					s.notifyWorker(notification.Payload)
				} else {
					s.notifyEvent(notification.Payload)
				}
			}
		}
		if conn != nil {
			conn.Close(context.Background())
		}
		if ctx.Err() != nil {
			return
		}
		// Reconnect backoff applies only to a broken notification connection.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// A wait is an idle connection renewal window, never a task deadline. Returning
// promptly on enqueue avoids client polling sleeps while keeping worker presence fresh.
func requestedWait(r *http.Request) time.Duration {
	for _, v := range strings.Split(r.Header.Get("Prefer"), ",") {
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, "wait=") {
			n, e := strconv.Atoi(strings.TrimPrefix(v, "wait="))
			if e == nil && n > 0 {
				return time.Duration(min(n, 20)) * time.Second
			}
		}
	}
	return 0
}
func (s *Store) claimWait(ctx context.Context, worker string, caps []string, p *WorkerProfile, wait time.Duration) (*ClaimedJob, error) {
	return s.claimSlotWait(ctx, worker, caps, p, 0, wait)
}
func (s *Store) claimSlotWait(ctx context.Context, worker string, caps []string, p *WorkerProfile, slot int, wait time.Duration) (*ClaimedJob, error) {
	request, err := prepareClaim(worker, caps, p, slot)
	if err != nil {
		return nil, err
	}
	if wait <= 0 {
		return s.claimPreparedSlot(ctx, request)
	}
	watch := s.watchClaims(worker)
	defer s.unwatchClaims(worker, watch)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		ch := s.claimChanges(watch)
		job, e := s.claimPreparedSlot(ctx, request)
		if !errors.Is(e, ErrNotFound) || wait == 0 {
			return job, e
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, ErrNotFound
		case <-ch:
		}
	}
}
func (a *API) waitJobs(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var req struct {
		IDs []string `json:"ids"`
	}
	if !a.decode(w, r, &req) {
		return
	}
	if len(req.IDs) == 0 {
		fail(w, ErrInvalid)
		return
	}
	for _, id := range req.IDs {
		if !uuidPattern.MatchString(id) {
			fail(w, ErrInvalid)
			return
		}
	}
	projection := `jsonb_build_object('id',j.id,'status',j.status,'result',(SELECT jsonb_build_object('status',r.status,'result',r.result,'error',r.error) FROM ` + a.Store.table("results") + ` r WHERE r.job_id=j.id))`
	if strings.Contains(r.Header.Get("Prefer"), "return=minimal") {
		projection = `jsonb_build_object('id',j.id,'status',j.status,'result_ref','/v1/admin/jobs/'||j.id::text||'/result')`
		w.Header().Set("Preference-Applied", "return=minimal")
	}
	query := `SELECT coalesce(jsonb_agg(` + projection + `),'[]'::jsonb) FROM ` + a.Store.table("jobs") + ` j WHERE j.id=ANY($1::uuid[]) AND j.status IN('succeeded','failed','cancelled')`
	timer := time.NewTimer(requestedWait(r))
	defer timer.Stop()
	for {
		ch := a.Store.changes()
		var b []byte
		e := a.Store.Pool.QueryRow(r.Context(), query, req.IDs).Scan(&b)
		if e != nil {
			fail(w, e)
			return
		}
		if string(b) != "[]" {
			writeJSON(w, 200, map[string]any{"jobs": json.RawMessage(b)})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			writeJSON(w, 200, map[string]any{"jobs": json.RawMessage(`[]`)})
			return
		case <-ch:
		}
	}
}

func (a *API) jobResult(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		fail(w, ErrInvalid)
		return
	}
	var b []byte
	e := a.Store.Pool.QueryRow(r.Context(), `SELECT jsonb_build_object('id',j.id,'status',j.status,'result',(SELECT jsonb_build_object('status',r.status,'result',r.result,'error',r.error) FROM `+a.Store.table("results")+` r WHERE r.job_id=j.id)) FROM `+a.Store.table("jobs")+` j WHERE j.id=$1`, id).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		e = ErrNotFound
	}
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, json.RawMessage(b))
}
