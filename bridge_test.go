package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"strings"
	"testing"
	"time"
)

func brokerFixture(t *testing.T) (*Store, *Bridge, *amqp.Connection) {
	t.Helper()
	url := os.Getenv("FLEET_TEST_AMQP_URL")
	if url == "" {
		t.Skip("FLEET_TEST_AMQP_URL not configured")
	}
	s := fixture(t)
	conn, e := amqp.Dial(url)
	if e != nil {
		t.Fatal("test RabbitMQ unavailable")
	}
	prefix := "fleet.test." + strings.ReplaceAll(newID(), "-", "")
	b := NewBridge(s, url)
	b.EventsExchange = prefix + ".events"
	b.CommandsExchange = prefix + ".commands"
	b.AuditQueue = prefix + ".audit"
	b.CommandQueue = prefix + ".gateway"
	ch, e := conn.Channel()
	must(t, e)
	must(t, b.declare(ch))
	ch.Close()
	t.Cleanup(func() {
		ch, e := conn.Channel()
		if e == nil {
			ch.QueueDelete(b.AuditQueue, false, false, false)
			ch.QueueDelete(b.CommandQueue, false, false, false)
			ch.ExchangeDelete(b.EventsExchange, false, false)
			ch.ExchangeDelete(b.CommandsExchange, false, false)
			ch.Close()
		}
		conn.Close()
	})
	return s, b, conn
}
func getBrokerMessage(t *testing.T, ch *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		d, ok, e := ch.Get(queue, false)
		must(t, e)
		if ok {
			return d
		}
		select {
		case <-ctx.Done():
			t.Fatal("broker message not available after network-operation wait")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func confirmPublish(t *testing.T, ch *amqp.Channel, exchange, key, id string, body []byte) {
	t.Helper()
	c, e := ch.PublishWithDeferredConfirmWithContext(context.Background(), exchange, key, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: id, Body: body})
	must(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ok, e := c.WaitContext(ctx)
	must(t, e)
	if !ok {
		t.Fatal("publication rejected")
	}
}
func TestBrokerMandatoryReturnDoesNotMarkOutboxPublished(t *testing.T) {
	s, b, conn := brokerFixture(t)
	ctx := context.Background()
	enqueue(t, s, "sha256", "unroutable")
	ch, e := conn.Channel()
	must(t, e)
	must(t, ch.Confirm(false))
	returned := ch.NotifyReturn(make(chan amqp.Return, 1))
	must(t, ch.QueueUnbind(b.AuditQueue, "#", b.EventsExchange, nil))
	sent, e := b.publishOne(ctx, ch, returned)
	if e == nil || sent {
		t.Fatal("unroutable positive confirm retired durable outbox", sent, e)
	}
	var n int
	must(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM `+s.table("outbox")+` WHERE published_at IS NULL`).Scan(&n))
	if n != 1 {
		t.Fatal("outbox lost", n)
	}
	ch.Close()
	ch, e = conn.Channel()
	must(t, e)
	defer ch.Close()
	must(t, ch.Confirm(false))
	returned = ch.NotifyReturn(make(chan amqp.Return, 1))
	must(t, ch.QueueBind(b.AuditQueue, "#", b.EventsExchange, false, nil))
	sent, e = b.publishOne(ctx, ch, returned)
	must(t, e)
	if !sent {
		t.Fatal("outbox did not recover")
	}
	d := getBrokerMessage(t, ch, b.AuditQueue)
	if d.DeliveryMode != amqp.Persistent || d.MessageId == "" {
		t.Fatal("event was not persistent")
	}
	must(t, d.Ack(false))
}
func TestBrokerCommandSurvivesTwentyFiveReconnectsAndCompletes(t *testing.T) {
	s, b, conn := brokerFixture(t)
	ctx := context.Background()
	producer, e := conn.Channel()
	must(t, e)
	defer producer.Close()
	must(t, producer.Confirm(false))
	command := Command{EventID: "broker-proof-event", Type: "job.enqueue", Payload: EnqueueJob{Operation: "sha256", Input: json.RawMessage(`{"text":"broker proof"}`)}}
	body, _ := json.Marshal(command)
	confirmPublish(t, producer, b.CommandsExchange, "job.enqueue", command.EventID, body)
	// Every unacknowledged channel close is a real broker redelivery. Quorum
	// queues must not discard the command at RabbitMQ's default poison limit20.
	for range 25 {
		ch, e := conn.Channel()
		must(t, e)
		d := getBrokerMessage(t, ch, b.CommandQueue)
		if d.MessageId != command.EventID {
			t.Fatal("wrong redelivery")
		}
		must(t, ch.Close())
	}
	consumer, e := conn.Channel()
	must(t, e)
	defer consumer.Close()
	d := getBrokerMessage(t, consumer, b.CommandQueue)
	must(t, b.accept(ctx, d))
	if count(t, s, "inbox") != 1 || count(t, s, "jobs") != 1 {
		t.Fatal("command did not commit before ACK")
	}
	confirmPublish(t, producer, b.CommandsExchange, "job.enqueue", command.EventID, body)
	d = getBrokerMessage(t, consumer, b.CommandQueue)
	must(t, b.accept(ctx, d))
	if count(t, s, "jobs") != 1 || count(t, s, "inbox") != 1 {
		t.Fatal("redelivery duplicated canonical job")
	}
	register(t, s, "broker-proof-worker")
	j, e := s.Claim(ctx, "broker-proof-worker", []string{"sha256"})
	must(t, e)
	sum := sha256.Sum256([]byte("broker proof"))
	result, _ := json.Marshal(map[string]string{"sha256": hex.EncodeToString(sum[:])})
	completion := done(j, "broker-proof-worker")
	completion.Result = result
	r, e := s.Complete(ctx, j.ID, completion)
	must(t, e)
	if !r.Committed {
		t.Fatal(r)
	}
	returns := producer.NotifyReturn(make(chan amqp.Return, 1))
	events := 0
	for {
		sent, e := b.publishOne(ctx, producer, returns)
		must(t, e)
		if !sent {
			break
		}
		events++
	}
	if events < 3 {
		t.Fatal("lifecycle events missing", events)
	}
	found := false
	for range events {
		d := getBrokerMessage(t, consumer, b.AuditQueue)
		var event struct {
			Type    string         `json:"type"`
			Payload map[string]any `json:"payload"`
		}
		must(t, json.Unmarshal(d.Body, &event))
		if event.Type == "job.succeeded" && event.Payload["job_id"] == j.ID {
			found = true
		}
		if d.DeliveryMode != amqp.Persistent {
			t.Fatal("nonpersistent lifecycle event")
		}
		must(t, d.Ack(false))
	}
	if !found {
		t.Fatal("completion never reached actual broker")
	}
	var pending int
	must(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM `+s.table("outbox")+` WHERE published_at IS NULL`).Scan(&pending))
	if pending != 0 {
		t.Fatal("outbox not drained")
	}
}
