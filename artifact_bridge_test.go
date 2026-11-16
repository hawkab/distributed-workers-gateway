package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"strings"
	"testing"
)

func TestArtifactBrokerPublishesOnlyReferenceAfterSealedData(t *testing.T) {
	s, b, conn := brokerFixture(t)
	ctx := context.Background()
	var e error
	s.Artifacts, e = NewArtifactFiles(t.TempDir())
	must(t, e)
	register(t, s, "stream-worker")
	enqueue(t, s, "network.http", "broker-stream")
	j, e := s.Claim(ctx, "stream-worker", []string{"network.http"})
	must(t, e)
	a := newUpload(t, s, j, "large-response")
	body := strings.Repeat("binary-never-in-rabbit-", 64*1024)
	chunk, e := s.PutArtifactChunk(ctx, a.UploadID, identity(j, "stream-worker"), 0, strings.NewReader(body))
	must(t, e)
	before := count(t, s, "outbox")
	ref, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: identity(j, "stream-worker"), Length: int64(len(body)), SHA256: chunk.SHA256})
	must(t, e)
	c := done(j, "stream-worker")
	c.ArtifactIDs = []string{ref.ArtifactID}
	_, e = s.Complete(ctx, j.ID, c)
	must(t, e)
	ch, e := conn.Channel()
	must(t, e)
	defer ch.Close()
	must(t, ch.Confirm(false))
	returned := ch.NotifyReturn(make(chan amqp.Return, 1))
	total := 0
	for {
		sent, e := b.publishOne(ctx, ch, returned)
		must(t, e)
		if !sent {
			break
		}
		total++
	}
	if total != before+2 {
		t.Fatal("seal/completion event duplicated or absent", total, before)
	}
	found := false
	for range total {
		d := getBrokerMessage(t, ch, b.AuditQueue)
		if len(d.Body) > 4096 || bytes.Contains(d.Body, []byte("binary-never-in-rabbit-")) {
			t.Fatal("binary body entered broker")
		}
		var event struct {
			Type    string `json:"type"`
			Payload struct {
				Artifact ArtifactReceipt `json:"artifact"`
			} `json:"payload"`
		}
		must(t, json.Unmarshal(d.Body, &event))
		if event.Type == "artifact.sealed" {
			found = true
			if event.Payload.Artifact.SHA256 != ref.SHA256 {
				t.Fatal("event points at wrong object")
			}
			f, _, e := s.OpenArtifact(ctx, ref.ArtifactID)
			must(t, e)
			sha, n, e := copyArtifact(ctx, io.Discard, f)
			f.Close()
			must(t, e)
			if sha != ref.SHA256 || n != ref.Length {
				t.Fatal("broker ACK preceded durable bytes")
			}
		}
		must(t, d.Ack(false))
	}
	if !found {
		t.Fatal("artifact reference event missing")
	}
}
