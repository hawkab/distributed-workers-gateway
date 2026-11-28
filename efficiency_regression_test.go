package gateway

import (
	"context"
	"testing"
	"time"
)

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestNotificationsOnlyWakeReadersWhoseStateCanChange(t *testing.T) {
	for _, c := range []struct {
		event         string
		claim, result bool
	}{
		{"artifact.sealed", false, false}, {"effect.started", false, false}, {"effect.completed", false, false},
		{"result.candidate_saved", false, false}, {"attempt.started", false, false},
		{"job.queued", true, false}, {"job.succeeded", false, true}, {"job.failed", false, true}, {"job.cancelled", false, true}, {"job.cancel_requested", false, true},
	} {
		t.Run(c.event, func(t *testing.T) {
			s := NewStore(nil, "")
			watch := s.watchClaims("owner")
			defer s.unwatchClaims("owner", watch)
			claim, result := s.claimChanges(watch), s.changes()
			s.notifyEvent(c.event)
			if channelClosed(claim) != c.claim || channelClosed(result) != c.result {
				t.Fatal("wrong subscribers awakened")
			}
			// Reconnection must reconcile every durable state, even if the hints were lost.
			nextClaim, nextResult := s.claimChanges(watch), s.changes()
			s.notify()
			if !channelClosed(nextClaim) || !channelClosed(nextResult) {
				t.Fatal("reconnect missed a reader")
			}
		})
	}
}

func TestActualCompletionsAndQueuedCancellationWakeWaitingReaders(t *testing.T) {
	for _, status := range []string{"succeeded", "failed", "cancelled", "cancelled-running"} {
		t.Run(status, func(t *testing.T) {
			s := fixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			initial := s.changes()
			go s.RunNotifications(ctx)
			select {
			case <-initial:
			case <-time.After(3 * time.Second):
				t.Fatal("notification listener did not start")
			}
			register(t, s, "owner")
			setup := s.watchClaims("setup")
			defer s.unwatchClaims("setup", setup)
			queued := s.claimChanges(setup)
			id := enqueue(t, s, "sha256", "actual-terminal-wakeup")
			select {
			case <-queued:
			case <-time.After(3 * time.Second):
				t.Fatal("queued hint not delivered before terminal check")
			}
			var job *ClaimedJob
			if status != "cancelled" {
				var e error
				job, e = s.Claim(ctx, "owner", []string{"sha256"})
				must(t, e)
			}
			watch := s.watchClaims("owner")
			defer s.unwatchClaims("owner", watch)
			other := s.watchClaims("other")
			defer s.unwatchClaims("other", other)
			result, claim := s.changes(), s.claimChanges(watch)
			otherClaim := s.claimChanges(other)
			if status == "cancelled" {
				must(t, s.Cancel(ctx, id))
			} else {
				c := done(job, "owner")
				c.Status = status
				if status == "cancelled-running" {
					must(t, s.Cancel(ctx, id))
					c.Status = "failed"
					c.Error = []byte(`{"message":"cancelled"}`)
				}
				if status == "failed" {
					c.Error = []byte(`{"message":"test failure"}`)
				}
				receipt, e := s.Complete(ctx, id, c)
				must(t, e)
				if status == "cancelled-running" && (!receipt.CandidateSaved || receipt.Committed) {
					t.Fatal("cancelled attempt receipt semantics changed")
				}
			}
			channels := []<-chan struct{}{result}
			if status != "cancelled" {
				channels = append(channels, claim)
			}
			for _, ch := range channels {
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					t.Fatal("committed terminal state did not wake readers")
				}
			}
			if channelClosed(otherClaim) {
				t.Fatal("another worker was awakened by this completion")
			}
		})
	}
}

func TestClaimWatchReferencesAreReleased(t *testing.T) {
	s := NewStore(nil, "")
	a, b := s.watchClaims("one"), s.watchClaims("one")
	if a != b || len(s.claimWaiters) != 1 {
		t.Fatal("watchers not shared")
	}
	s.unwatchClaims("one", a)
	if len(s.claimWaiters) != 1 {
		t.Fatal("live watcher removed")
	}
	s.unwatchClaims("one", b)
	if s.claimWaiters != nil {
		t.Fatal("idle worker registry retained")
	}
}

func TestLiveLeaseDoesNotEvaluateResourceMatcher(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "owner")
	register(t, s, "next")
	profile := &WorkerProfile{Schema: 1, Capabilities: []string{"sha256"}, Resources: &WorkerResources{GPUStatus: "none"}}
	id := enqueue(t, s, "sha256", "live-resource-guard")
	_, e := s.ClaimWithProfile(ctx, "owner", nil, profile)
	must(t, e)
	_, e = s.Pool.Exec(ctx, `UPDATE `+s.table("jobs")+` SET priority=10,resource_requirements='{"cpu_cores":2}' WHERE id=$1`, id)
	must(t, e)
	// A deliberately expensive/failing matcher exposes any evaluation of an
	// ineligible live job. The next queued job must still be claimable.
	_, e = s.Pool.Exec(ctx, `CREATE OR REPLACE FUNCTION `+s.table("resources_match")+`(p jsonb,r jsonb) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN IF r->>'cpu_cores'='2' THEN RAISE EXCEPTION 'live job evaluated'; END IF; RETURN true; END; $$`)
	must(t, e)
	next := enqueue(t, s, "sha256", "eligible-after-live")
	got, e := s.ClaimWithProfile(ctx, "next", nil, profile)
	must(t, e)
	if got.ID != next {
		t.Fatal("priority or eligible selection changed")
	}
}
