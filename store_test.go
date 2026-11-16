package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func fixture(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("REGISTRY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("REGISTRY_TEST_DATABASE_URL not configured")
	}
	ctx := context.Background()
	pool, e := pgxpool.New(ctx, url)
	must(t, e)
	schema := "fleet_test_" + strings.ReplaceAll(newID(), "-", "")
	s := NewStore(pool, schema)
	must(t, s.Init(ctx))
	t.Cleanup(func() {
		_, e := pool.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		if e != nil {
			t.Error("test schema cleanup failed")
		}
		pool.Close()
	})
	return s
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func register(t *testing.T, s *Store, id string) string {
	t.Helper()
	token := NewToken()
	must(t, s.RegisterWorker(context.Background(), id, token))
	return token
}
func enqueue(t *testing.T, s *Store, op, key string) string {
	t.Helper()
	id, e := s.Enqueue(context.Background(), EnqueueJob{Operation: op, Input: json.RawMessage(`{"text":"hello"}`), IdempotencyKey: key})
	must(t, e)
	return id
}
func expire(t *testing.T, s *Store, id string) {
	t.Helper()
	_, e := s.Pool.Exec(context.Background(), `UPDATE `+s.table("attempts")+` SET lease_until=now()-interval '1 second' WHERE id=$1`, id)
	must(t, e)
}
func done(j *ClaimedJob, worker string) Complete {
	return Complete{WorkerID: worker, AttemptID: j.AttemptID, FencingToken: j.FencingToken, Status: "succeeded", Result: json.RawMessage(`{"sha256":"abc","count":1}`)}
}
func count(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	must(t, s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM `+s.table(table)).Scan(&n))
	return n
}
func TestConcurrentClaimAndCompletionAreIdempotent(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "tv-1")
	jobID := enqueue(t, s, "sha256", "request-1")
	const parallel = 16
	out := make(chan *ClaimedJob, parallel)
	errs := make(chan error, parallel)
	var wg sync.WaitGroup
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, e := s.Claim(ctx, "tv-1", []string{"sha256"})
			if e != nil {
				errs <- e
			} else {
				out <- j
			}
		}()
	}
	wg.Wait()
	close(out)
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	var first *ClaimedJob
	for j := range out {
		if first == nil {
			first = j
		}
		if j.ID != jobID || j.AttemptID != first.AttemptID || j.FencingToken != 1 {
			t.Fatal("duplicate claim assigned multiple executions")
		}
	}
	if count(t, s, "attempts") != 1 {
		t.Fatal("duplicate attempts")
	}
	ack := make(chan CompletionReceipt, parallel)
	errs = make(chan error, parallel)
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.Complete(ctx, jobID, done(first, "tv-1"))
			if e != nil {
				errs <- e
			} else {
				ack <- r
			}
		}()
	}
	wg.Wait()
	close(ack)
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	for a := range ack {
		if !a.Committed || a.CandidateSaved {
			t.Fatal(a)
		}
	}
	if count(t, s, "results") != 1 || count(t, s, "candidates") != 0 {
		t.Fatal("completion was duplicated")
	}
	var n int
	must(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM `+s.table("outbox")+` WHERE event_type='job.succeeded'`).Scan(&n))
	if n != 1 {
		t.Fatal("duplicate completion events", n)
	}
	c := done(first, "tv-1")
	c.Result = json.RawMessage(`{"count":1, "sha256":"abc"}`)
	r, e := s.Complete(ctx, jobID, c)
	must(t, e)
	if !r.Committed {
		t.Fatal(r)
	}
	c.Result = json.RawMessage(`{"sha256":"different"}`)
	_, e = s.Complete(ctx, jobID, c)
	if !errors.Is(e, ErrConflict) {
		t.Fatal("changed completion must conflict", e)
	}
}
func TestClaimCapabilityAndSingleOwnerAcrossWorkers(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	for _, w := range []string{"tv-a", "tv-b", "unsupported"} {
		register(t, s, w)
	}
	id := enqueue(t, s, "javascript", "javascript-1")
	if _, e := s.Claim(ctx, "unsupported", []string{"sha256"}); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan *ClaimedJob, 2)
	errorsCh := make(chan error, 2)
	for _, w := range []string{"tv-a", "tv-b"} {
		wg.Add(1)
		go func(w string) {
			defer wg.Done()
			j, e := s.Claim(ctx, w, []string{"javascript"})
			if e == nil {
				results <- j
			} else {
				errorsCh <- e
			}
		}(w)
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	n := 0
	for j := range results {
		n++
		if j.ID != id {
			t.Fatal(j)
		}
	}
	if n != 1 {
		t.Fatal("multiple owners", n)
	}
	for e := range errorsCh {
		if !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
	}
}
func TestExpiredLeaseFencesAndRetainsOldResultBeforeReceipt(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "old")
	register(t, s, "new")
	id := enqueue(t, s, "sha256", "lost-worker")
	old, e := s.Claim(ctx, "old", []string{"sha256"})
	must(t, e)
	expire(t, s, old.AttemptID)
	fresh, e := s.Claim(ctx, "new", []string{"sha256"})
	must(t, e)
	if fresh.ID != id || fresh.FencingToken != 2 || fresh.AttemptID == old.AttemptID {
		t.Fatal(fresh)
	}
	if _, e = s.JobHeartbeat(ctx, id, JobHeartbeat{WorkerID: "old", AttemptID: old.AttemptID, FencingToken: old.FencingToken}); !errors.Is(e, ErrLeaseLost) {
		t.Fatal(e)
	}
	receipt, e := s.Complete(ctx, id, done(old, "old"))
	must(t, e)
	if receipt.Committed || !receipt.CandidateSaved {
		t.Fatal(receipt)
	}
	if count(t, s, "candidates") != 1 || count(t, s, "results") != 0 {
		t.Fatal("candidate was not durable before receipt")
	}
	receipt2, e := s.Complete(ctx, id, done(old, "old"))
	must(t, e)
	if receipt2 != receipt || count(t, s, "candidates") != 1 {
		t.Fatal("candidate receipt not idempotent")
	}
	receipt, e = s.Complete(ctx, id, done(fresh, "new"))
	must(t, e)
	if !receipt.Committed || count(t, s, "results") != 1 || count(t, s, "candidates") != 1 {
		t.Fatal(receipt)
	}
}
func TestLongJobRenewsWithoutOverallDeadlineAndPreservesNewAttempt(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "tv")
	id := enqueue(t, s, "sha256", "long-job")
	j, e := s.Claim(ctx, "tv", []string{"sha256"})
	must(t, e)
	_, e = s.Pool.Exec(ctx, `UPDATE `+s.table("attempts")+` SET started_at=now()-interval '30 days',lease_until=now()+interval '10 seconds' WHERE id=$1`, j.AttemptID)
	must(t, e)
	receipt, e := s.JobHeartbeat(ctx, id, JobHeartbeat{WorkerID: "tv", AttemptID: j.AttemptID, FencingToken: j.FencingToken, Progress: json.RawMessage(`{"rows":500000}`)})
	must(t, e)
	if time.Until(receipt.LeaseUntil) < 110*time.Second {
		t.Fatal("long job was not renewed")
	}
	expire(t, s, j.AttemptID)
	next, e := s.Claim(ctx, "tv", []string{"sha256"})
	must(t, e)
	if next.FencingToken != 2 {
		t.Fatal(next)
	}
	_, e = s.Complete(ctx, id, done(j, "tv"))
	must(t, e)
	var active, state string
	must(t, s.Pool.QueryRow(ctx, `SELECT active_job_id,state FROM `+s.table("workers")+` WHERE id='tv'`).Scan(&active, &state))
	if active != id || state != "busy" {
		t.Fatal("stale completion cleared current work", active, state)
	}
}
func TestEnqueueIdempotencyAndDurableInbox(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	id := enqueue(t, s, "sha256", "same")
	id2 := enqueue(t, s, "sha256", "same")
	if id != id2 {
		t.Fatal("same key duplicate")
	}
	_, e := s.Enqueue(ctx, EnqueueJob{Operation: "text.normalize", Input: json.RawMessage(`{}`), IdempotencyKey: "same"})
	if !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	c := Command{EventID: "event-123", Type: "job.enqueue", Payload: EnqueueJob{Operation: "text.normalize", Input: json.RawMessage(`{"text":" x "}`)}}
	a, e := s.AcceptCommand(ctx, c)
	must(t, e)
	b, e := s.AcceptCommand(ctx, c)
	must(t, e)
	if !a.Accepted || a != b || count(t, s, "inbox") != 1 || count(t, s, "jobs") != 2 {
		t.Fatal(a, b)
	}
	c.Payload.Input = json.RawMessage(`{"text":"other"}`)
	b, e = s.AcceptCommand(ctx, c)
	must(t, e)
	if b.Accepted || b.Error != "event_id_conflict" || count(t, s, "inbox_conflicts") != 1 || count(t, s, "jobs") != 2 {
		t.Fatal(b)
	}
	c.EventID = "unsupported-event"
	c.Type = "destroy_everything"
	b, e = s.AcceptCommand(ctx, c)
	must(t, e)
	if b.Accepted || count(t, s, "inbox") != 2 {
		t.Fatal(b)
	}
	must(t, s.RejectCommand(ctx, []byte("not JSON"), "invalid_json"))
	must(t, s.RejectCommand(ctx, []byte("not JSON"), "invalid_json"))
	if count(t, s, "inbox") != 3 {
		t.Fatal("invalid broker body lost or duplicated")
	}
}
func TestCancelPolicyAndAdministrativeMetrics(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "tv")
	id := enqueue(t, s, "sha256", "queued-cancel")
	must(t, s.Cancel(ctx, id))
	if _, e := s.Claim(ctx, "tv", []string{"sha256"}); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	id = enqueue(t, s, "sha256", "running-cancel")
	j, e := s.Claim(ctx, "tv", []string{"sha256"})
	must(t, e)
	must(t, s.Cancel(ctx, id))
	receipt, e := s.JobHeartbeat(ctx, id, JobHeartbeat{WorkerID: "tv", AttemptID: j.AttemptID, FencingToken: j.FencingToken})
	must(t, e)
	if !receipt.CancelRequested {
		t.Fatal(receipt)
	}
	must(t, s.SetWorkerPolicy(ctx, "tv", 30))
	policy, e := s.WorkerPolicy(ctx, "tv")
	must(t, e)
	if policy != 30 {
		t.Fatal(policy)
	}
	m, e := s.Metrics(ctx)
	must(t, e)
	if !strings.Contains(string(m), "outbox_pending") {
		t.Fatal("metrics missing")
	}
	w, e := s.Workers(ctx)
	must(t, e)
	if strings.Contains(string(w), "token_hash") {
		t.Fatal("credential hash leaked")
	}
	b, e := s.Job(ctx, id)
	must(t, e)
	if !strings.Contains(string(b), "attempts") {
		t.Fatal("history missing")
	}
	_, e = s.Job(ctx, newID())
	if !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
}
func TestUUIDAndCanonicalJSON(t *testing.T) {
	if !uuidPattern.MatchString(newID()) {
		t.Fatal("bad job identity")
	}
	for _, raw := range []string{`{"n":9007199254740993}`, `[1,true,null,"text"]`, `null`} {
		b, e := canonical(json.RawMessage(raw))
		must(t, e)
		if !json.Valid(b) {
			t.Fatal(fmt.Sprint(b))
		}
	}
}

func TestLeaseExpirationAfterLockWaitCannotResurrectOrCommit(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "locked")
	id := enqueue(t, s, "sha256", "lockwait")
	j, e := s.Claim(ctx, "locked", []string{"sha256"})
	must(t, e)
	// Hold the worker row so the API transaction starts before expiry, but cannot
	// inspect the lease until after expiry. Wall-clock checks must reject it.
	blocker, e := s.Pool.Begin(ctx)
	must(t, e)
	must(t, s.lockWorker(ctx, blocker, "locked"))
	defer blocker.Rollback(context.Background())
	var expiresAt time.Time
	e = blocker.QueryRow(ctx, `UPDATE `+s.table("attempts")+` SET lease_until=clock_timestamp()+interval '1 second' WHERE id=$1 RETURNING lease_until`, j.AttemptID).Scan(&expiresAt)
	must(t, e)
	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(entered)
		_, e := s.JobHeartbeat(ctx, id, JobHeartbeat{WorkerID: "locked", AttemptID: j.AttemptID, FencingToken: j.FencingToken})
		result <- e
	}()
	<-entered
	// Observe that the heartbeat is actually waiting for the held row lock.
	for {
		var waiting bool
		var started *time.Time
		must(t, s.Pool.QueryRow(ctx, `SELECT count(*)>0,min(query_start) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE $1 AND pid<>pg_backend_pid()`, "SELECT id FROM "+s.table("workers")+"%").Scan(&waiting, &started))
		if waiting {
			if started == nil || !started.Before(expiresAt) {
				t.Fatal("control did not begin before expiry")
			}
			break
		}
		select {
		case err := <-result:
			t.Fatal("heartbeat did not wait for held lock", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	for {
		var current time.Time
		must(t, s.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&current))
		if !current.Before(expiresAt) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	must(t, blocker.Commit(ctx))
	if e = <-result; !errors.Is(e, ErrLeaseLost) {
		t.Fatal("expired heartbeat renewed after lock wait", e)
	}
	receipt, e := s.Complete(ctx, id, done(j, "locked"))
	must(t, e)
	if receipt.Committed || !receipt.CandidateSaved {
		t.Fatal(receipt)
	}
}
func TestCancelledLostWorkerReconcilesWithoutDiscardingResult(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "tv")
	id := enqueue(t, s, "sha256", "cancel-lost")
	j, e := s.Claim(ctx, "tv", []string{"sha256"})
	must(t, e)
	must(t, s.Cancel(ctx, id))
	expire(t, s, j.AttemptID)
	must(t, s.ReconcileCancelled(ctx))
	var status string
	must(t, s.Pool.QueryRow(ctx, `SELECT status FROM `+s.table("jobs")+` WHERE id=$1`, id).Scan(&status))
	if status != "cancelled" {
		t.Fatal(status)
	}
	receipt, e := s.Complete(ctx, id, done(j, "tv"))
	must(t, e)
	if receipt.Committed || !receipt.CandidateSaved {
		t.Fatal("cancelled stale result lost", receipt)
	}
}

func TestInitWorksForPrecreatedSchemaOwnerWithoutDatabaseCreate(t *testing.T) {
	admin := fixture(t)
	ctx := context.Background()
	role := "fleet_role_" + strings.ReplaceAll(newID(), "-", "")
	schema := "fleet_owned_" + strings.ReplaceAll(newID(), "-", "")
	_, e := admin.Pool.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` NOLOGIN`)
	if e != nil {
		t.Skip("role-isolation control requires CREATE ROLE in test database")
	}
	t.Cleanup(func() {
		admin.Pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		if _, e := admin.Pool.Exec(context.Background(), `DROP ROLE `+pgx.Identifier{role}.Sanitize()); e != nil {
			t.Error("test role cleanup failed")
		}
	})
	_, e = admin.Pool.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()+` AUTHORIZATION `+pgx.Identifier{role}.Sanitize())
	must(t, e)
	cfg := admin.Pool.Config().Copy()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, e := c.Exec(ctx, `SET ROLE `+pgx.Identifier{role}.Sanitize())
		return e
	}
	restricted, e := pgxpool.NewWithConfig(ctx, cfg)
	must(t, e)
	defer restricted.Close()
	var createDB bool
	must(t, restricted.QueryRow(ctx, `SELECT has_database_privilege(current_user,current_database(),'CREATE')`).Scan(&createDB))
	if createDB {
		t.Skip("test database PUBLIC has CREATE; restricted DB fixture not available")
	}
	store := NewStore(restricted, schema)
	must(t, store.Init(ctx))
	register(t, store, "restricted-tv")
	enqueue(t, store, "sha256", "restricted-proof")
}
