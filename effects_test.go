package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func beginEffect(j *ClaimedJob, worker, execution string, raw string) EffectBegin {
	return EffectBegin{AttemptIdentity: identity(j, worker), ExecutionID: execution, Request: json.RawMessage(raw)}
}
func finishEffect(j *ClaimedJob, worker, execution, sha string) EffectFinish {
	return EffectFinish{AttemptIdentity: identity(j, worker), ExecutionID: execution, RequestSHA256: sha, Response: json.RawMessage(`{"status":201,"headers":{"content-type":["application/json"]}}`)}
}
func sameEffectJSON(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	one, e := canonical(a)
	must(t, e)
	two, e := canonical(b)
	must(t, e)
	return bytes.Equal(one, two)
}
func TestNeverEffectLiveNonceRetryCrashUncertainAndLateResponseReplay(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	execution := newID()
	original := `{"method":"POST","url":"http://127.0.0.1/private","body_text":"mutation"}`
	request := beginEffect(j, "stream-worker", execution, original)
	first, e := s.BeginEffect(ctx, j.ID, "request", request)
	must(t, e)
	if first.State != "execute" {
		t.Fatal(first)
	}
	same, e := s.BeginEffect(ctx, j.ID, "request", request)
	must(t, e)
	if !reflect.DeepEqual(first, same) {
		t.Fatal("lost begin ACK changed execution grant")
	}
	request.ExecutionID = newID()
	uncertain, e := s.BeginEffect(ctx, j.ID, "request", request)
	must(t, e)
	if uncertain.State != "uncertain" {
		t.Fatal("process restart could repeat mutation", uncertain)
	}
	expire(t, s, j.AttemptID)
	register(t, s, "replacement")
	next, e := s.Claim(ctx, "replacement", []string{"network.http"})
	must(t, e)
	newRequest := beginEffect(next, "replacement", newID(), original)
	uncertain, e = s.BeginEffect(ctx, j.ID, "request", newRequest)
	must(t, e)
	if uncertain.State != "uncertain" {
		t.Fatal("reassigned mutation reran")
	}
	upload := newUpload(t, s, j, "late-http")
	_, e = s.PutArtifactChunk(ctx, upload.UploadID, identity(j, "stream-worker"), 0, bytes.NewBufferString("external-result"))
	must(t, e)
	artifact, e := s.FinishArtifact(ctx, upload.UploadID, FinishArtifact{AttemptIdentity: identity(j, "stream-worker"), Length: 15})
	must(t, e)
	if !artifact.CandidateSaved {
		t.Fatal(artifact)
	}
	finish := finishEffect(j, "stream-worker", execution, first.RequestSHA256)
	finish.ArtifactIDs = []string{artifact.ArtifactID}
	ack, e := s.FinishEffect(ctx, j.ID, "request", finish)
	must(t, e)
	if !ack.Recorded || !ack.Committed {
		t.Fatal("real late external result discarded", ack)
	}
	duplicate, e := s.FinishEffect(ctx, j.ID, "request", finish)
	must(t, e)
	if !reflect.DeepEqual(ack, duplicate) {
		t.Fatal("effect finish receipt not idempotent")
	}
	replay, e := s.BeginEffect(ctx, j.ID, "request", newRequest)
	must(t, e)
	if replay.State != "completed" || len(replay.ArtifactIDs) != 1 || !sameEffectJSON(t, replay.Response, finish.Response) {
		t.Fatal(replay)
	}
	c := done(next, "replacement")
	c.ArtifactIDs = replay.ArtifactIDs
	result, e := s.Complete(ctx, j.ID, c)
	must(t, e)
	if !result.Committed {
		t.Fatal("current attempt could not adopt stored external response", result)
	}
}
func TestSafeEffectTakeoverPreservesLateCandidateAndUnknownExecutionDenied(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	oldExecution := newID()
	raw := `{"url":"http://localhost/read"}`
	old, e := s.BeginEffect(ctx, j.ID, "read", beginEffect(j, "stream-worker", oldExecution, raw))
	must(t, e)
	expire(t, s, j.AttemptID)
	register(t, s, "other")
	next, e := s.Claim(ctx, "other", []string{"network.http"})
	must(t, e)
	newExecution := newID()
	fresh, e := s.BeginEffect(ctx, j.ID, "read", beginEffect(next, "other", newExecution, raw))
	must(t, e)
	if fresh.State != "execute" || fresh.RequestSHA256 != old.RequestSHA256 {
		t.Fatal(fresh)
	}
	if _, e = s.BeginEffect(ctx, j.ID, "read", beginEffect(j, "stream-worker", oldExecution, raw)); !errors.Is(e, ErrLeaseLost) {
		t.Fatal("expired owner regained execution", e)
	}
	if _, e = s.BeginEffect(ctx, j.ID, "read", beginEffect(next, "other", oldExecution, raw)); !errors.Is(e, ErrConflict) {
		t.Fatal("retired nonce reclaimed newest execution", e)
	}
	forged := finishEffect(j, "stream-worker", newID(), old.RequestSHA256)
	if _, e = s.FinishEffect(ctx, j.ID, "read", forged); !errors.Is(e, ErrUnauthorized) {
		t.Fatal("unreserved execution recorded", e)
	}
	late := finishEffect(j, "stream-worker", oldExecution, old.RequestSHA256)
	late.Response = json.RawMessage(`{"status":200,"old":true}`)
	receipt, e := s.FinishEffect(ctx, j.ID, "read", late)
	must(t, e)
	if !receipt.CandidateSaved || receipt.Committed {
		t.Fatal("old result replaced current owner", receipt)
	}
	again, e := s.FinishEffect(ctx, j.ID, "read", late)
	must(t, e)
	if !reflect.DeepEqual(receipt, again) || count(t, s, "effect_candidates") != 1 {
		t.Fatal("late response redelivery duplicated")
	}
	final := finishEffect(next, "other", newExecution, fresh.RequestSHA256)
	final.Response = json.RawMessage(`{"status":200,"new":true}`)
	_, e = s.FinishEffect(ctx, j.ID, "read", final)
	must(t, e)
	replay, e := s.BeginEffect(ctx, j.ID, "read", beginEffect(next, "other", newID(), raw))
	must(t, e)
	if replay.State != "completed" || !sameEffectJSON(t, replay.Response, final.Response) {
		t.Fatal("current response clobbered", replay)
	}
}
func TestEffectServerCanonicalHashAndVendorIdempotencyPolicy(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	id := newID()
	one := `{"method":"post","url":"http://127.0.0.1/write","replay":"idempotent","idempotency_key":"stable-key","headers":{"Idempotency-Key":"stable-key"}}`
	first, e := s.BeginEffect(ctx, j.ID, "write", beginEffect(j, "stream-worker", id, one))
	must(t, e)
	two := `{"effect_id":"write","headers":{"idempotency-key":["stable-key"],"accept":["*/*"],"accept-encoding":["identity"],"user-agent":["DistributedWorker/1"]},"idempotency_key":"stable-key","replay":"idempotent","method":"POST","url":"http://127.0.0.1/write"}`
	next, e := s.BeginEffect(ctx, j.ID, "write", beginEffect(j, "stream-worker", newID(), two))
	must(t, e)
	if first.RequestSHA256 != next.RequestSHA256 || next.State != "execute" {
		t.Fatal("server hash depends on JSON/header spelling", first, next)
	}
	if _, e = s.BeginEffect(ctx, j.ID, "write", beginEffect(j, "stream-worker", id, one)); !errors.Is(e, ErrConflict) {
		t.Fatal("retired same-owner nonce reclaimed execution", e)
	}
	bad := []string{`{"method":"POST","url":"http://localhost/write","replay":"safe"}`, `{"method":"POST","url":"http://localhost/write","replay":"idempotent","idempotency_key":"x"}`, `{"method":"POST","url":"http://localhost/write","replay":"idempotent","idempotency_key":"x","headers":{"Idempotency-Key":"different"}}`}
	for _, raw := range bad {
		if _, e = s.BeginEffect(ctx, j.ID, "invalid", beginEffect(j, "stream-worker", newID(), raw)); !errors.Is(e, ErrInvalid) {
			t.Fatal("unsafe replay policy accepted", e)
		}
	}
	changed := `{"method":"POST","url":"http://127.0.0.1/write","body_text":"different"}`
	if _, e = s.BeginEffect(ctx, j.ID, "write", beginEffect(j, "stream-worker", newID(), changed)); !errors.Is(e, ErrConflict) {
		t.Fatal("effect identity changed after reservation", e)
	}
}
func TestOptionalNetworkGrantRoundTripsWithoutBlanketLANRestriction(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "grant-worker")
	grant := json.RawMessage(`{"origins":[],"methods":["GET"],"request_headers":[],"response_modes":["artifact"]}`)
	_, e := s.Enqueue(ctx, EnqueueJob{Operation: "network.http", Input: json.RawMessage(`{"url":"http://127.0.0.1:8080"}`), Network: grant})
	must(t, e)
	j, e := s.Claim(ctx, "grant-worker", []string{"network.http"})
	must(t, e)
	expected, _ := canonical(grant)
	actual, _ := canonical(j.Network)
	if !bytes.Equal(expected, actual) {
		t.Fatal("absent/empty grant semantics changed", string(j.Network))
	}
}
