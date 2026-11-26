package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestConcurrentSlotsFencingAndReducedBudget(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	must(t, s.RegisterWorker(ctx, "parallel", NewToken()))
	p := testProfile(t, `{"cpu_available_cores":8,"ram_available_bytes":1073741824}`, "sha256")
	p.Execution = &ExecutionBudget{MaxConcurrentJobs: 4}
	for i := 0; i < 8; i++ {
		_, e := s.Enqueue(ctx, EnqueueJob{Operation: "sha256"})
		must(t, e)
	}
	var wg sync.WaitGroup
	jobs := make([]*ClaimedJob, 4)
	for n := range jobs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			j, e := s.ClaimSlot(ctx, "parallel", nil, p, n)
			if e != nil {
				t.Error(e)
			}
			jobs[n] = j
		}(n)
	}
	wg.Wait()
	seen := map[string]bool{}
	for n, j := range jobs {
		if j == nil || seen[j.ID] {
			t.Fatal("duplicate/missing", jobs)
		}
		seen[j.ID] = true
		again, e := s.ClaimSlot(ctx, "parallel", nil, p, n)
		must(t, e)
		if again.ID != j.ID || again.AttemptID != j.AttemptID {
			t.Fatal("uncertain claim duplicated")
		}
	}
	p.Execution = &ExecutionBudget{MaxConcurrentJobs: 1}
	j, e := s.ClaimSlot(ctx, "parallel", nil, p, 3)
	must(t, e)
	if j.ID != jobs[3].ID {
		t.Fatal("reduced budget abandoned live work")
	}
	for _, j := range jobs {
		r, e := s.Complete(ctx, j.ID, Complete{WorkerID: "parallel", AttemptID: j.AttemptID, FencingToken: j.FencingToken, Status: "succeeded"})
		must(t, e)
		if !r.Committed {
			t.Fatal("not committed")
		}
	}
	if _, e := s.ClaimSlot(ctx, "parallel", nil, p, 1); !errors.Is(e, ErrNotFound) {
		t.Fatal("shrunken slot accepted", e)
	}
	if _, e := s.ClaimSlot(ctx, "parallel", nil, nil, 1); !errors.Is(e, ErrNotFound) {
		t.Fatal("legacy admitted parallel work", e)
	}
	p.Execution = &ExecutionBudget{MaxConcurrentJobs: 0}
	if _, e := s.ClaimSlot(ctx, "parallel", nil, p, 0); !errors.Is(e, ErrNotFound) {
		t.Fatal("pressure pause ignored", e)
	}
}
