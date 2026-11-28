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
		{"completion.candidate_saved", false, false}, {"attempt.started", false, false},
		{"job.queued", true, false}, {"job.succeeded", true, true}, {"job.failed", true, true}, {"job.cancelled", true, true}, {"job.cancel_requested", true, true},
	} {
		t.Run(c.event, func(t *testing.T) {
			s := NewStore(nil, "")
			claim, result := s.claimChanges(), s.changes()
			s.notifyEvent(c.event)
			if channelClosed(claim) != c.claim || channelClosed(result) != c.result {
				t.Fatal("wrong subscribers awakened")
			}
			// Reconnection must reconcile every durable state, even if the hints were lost.
			nextClaim, nextResult := s.claimChanges(), s.changes()
			s.notify()
			if !channelClosed(nextClaim) || !channelClosed(nextResult) {
				t.Fatal("reconnect missed a reader")
			}
		})
	}
}

func TestActualCompletionsAndQueuedCancellationWakeWaitingReaders(t *testing.T) {
	for _, status := range []string{"succeeded", "failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			s := fixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			initial := s.claimChanges()
			go s.RunNotifications(ctx)
			select {
			case <-initial:
			case <-time.After(3 * time.Second):
				t.Fatal("notification listener did not start")
			}
			register(t, s, "owner")
			id := enqueue(t, s, "sha256", "actual-terminal-wakeup")
			var job *ClaimedJob
			if status != "cancelled" {
				var e error
				job, e = s.Claim(ctx, "owner", []string{"sha256"})
				must(t, e)
			}
			result, claim := s.changes(), s.claimChanges()
			if status == "cancelled" {
				must(t, s.Cancel(ctx, id))
			} else {
				c := done(job, "owner")
				c.Status = status
				if status == "failed" {
					c.Error = []byte(`{"message":"test failure"}`)
				}
				_, e := s.Complete(ctx, id, c)
				must(t, e)
			}
			for _, ch := range []<-chan struct{}{result, claim} {
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					t.Fatal("committed terminal state did not wake readers")
				}
			}
		})
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
