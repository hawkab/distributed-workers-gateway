package gateway

import (
	"context"
	"errors"
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
