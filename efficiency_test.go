package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func BenchmarkCopyArtifact(b *testing.B) {
	data := bytes.Repeat([]byte("streamed-artifact-content"), 45000)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		_, n, e := copyArtifact(context.Background(), io.Discard, bytes.NewReader(data))
		if e != nil || n != int64(len(data)) {
			b.Fatal(n, e)
		}
	}
}

func BenchmarkIdleClaimLiveJobs(b *testing.B) {
	dsn := os.Getenv("REGISTRY_TEST_DATABASE_URL")
	if dsn == "" {
		b.Fatal("isolated PostgreSQL is required")
	}
	ctx := context.Background()
	pool, e := pgxpool.New(ctx, dsn)
	if e != nil {
		b.Fatal(e)
	}
	s := NewStore(pool, "fleet_bench_"+strings.ReplaceAll(newID(), "-", ""))
	s.Lease = time.Hour
	if e = s.Init(ctx); e != nil {
		b.Fatal(e)
	}
	b.Cleanup(func() {
		_, e := pool.Exec(ctx, `DROP SCHEMA `+pgx.Identifier{s.Schema}.Sanitize()+` CASCADE`)
		if e != nil {
			b.Error(e)
		}
		pool.Close()
	})
	for _, id := range []string{"owner", "idle"} {
		if e = s.RegisterWorker(ctx, id, strings.Repeat(id, 32)); e != nil {
			b.Fatal(e)
		}
	}
	profile := &WorkerProfile{Schema: 1, Capabilities: []string{"sha256"}, Resources: &WorkerResources{GPUStatus: "none"}, Execution: &ExecutionBudget{MaxConcurrentJobs: 64}}
	for i := 0; i < 59; i++ {
		_, e = s.Enqueue(ctx, EnqueueJob{Operation: "sha256", Input: []byte(`{}`), RequiredCapabilities: []string{"sha256"}})
		if e != nil {
			b.Fatal(e)
		}
		if _, e = s.ClaimSlot(ctx, "owner", nil, profile, i); e != nil {
			b.Fatal(e)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, e := s.ClaimWithProfile(ctx, "idle", nil, profile); !errors.Is(e, ErrNotFound) {
			b.Fatal(e)
		}
	}
}
