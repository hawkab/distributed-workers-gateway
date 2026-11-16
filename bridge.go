package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"sync"
	"time"
)

// Bridge uses this fleet's dedicated broker/vhost only. The PostgreSQL ledger
// remains canonical; publisher confirms and inbox dedup make redelivery safe.
type Bridge struct {
	Store                                                      *Store
	URL                                                        string
	EventsExchange, CommandsExchange, AuditQueue, CommandQueue string
}

func NewBridge(s *Store, url string) *Bridge {
	return &Bridge{Store: s, URL: url, EventsExchange: "fleet.events", CommandsExchange: "fleet.commands", AuditQueue: "fleet.events.audit", CommandQueue: "fleet.commands.gateway"}
}
func (b *Bridge) Run(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		e := b.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if e != nil {
			log.Print("fleet broker connection unavailable; durable outbox will retry")
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(30*time.Second, delay*2)
	}
}
func (b *Bridge) session(ctx context.Context) error {
	conn, e := amqp.DialConfig(b.URL, amqp.Config{Heartbeat: 30 * time.Second, Dial: amqp.DefaultDial(10 * time.Second)})
	if e != nil {
		return e
	}
	defer conn.Close()
	pub, e := conn.Channel()
	if e != nil {
		return e
	}
	defer pub.Close()
	sub, e := conn.Channel()
	if e != nil {
		return e
	}
	defer sub.Close()
	if e = b.declare(pub); e != nil {
		return e
	}
	returned := pub.NotifyReturn(make(chan amqp.Return, 1))
	if e = pub.Confirm(false); e != nil {
		return e
	}
	if e = sub.Qos(1, 0, false); e != nil {
		return e
	}
	deliveries, e := sub.Consume(b.CommandQueue, "", false, false, false, false, nil)
	if e != nil {
		return e
	}
	session, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for session.Err() == nil {
			sent, err := b.publishOne(session, pub, returned)
			if err != nil {
				failures <- err
				return
			}
			if !sent {
				timer := time.NewTimer(500 * time.Millisecond)
				select {
				case <-session.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}()
	defer func() { cancel(); conn.Close(); wg.Wait() }()
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	log.Print("fleet broker bridge ready")
	for {
		select {
		case <-session.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case err := <-failures:
				return err
			default:
				return errors.New("publisher stopped")
			}
		case e := <-closed:
			if e != nil {
				return e
			}
			return errors.New("broker connection closed")
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("broker consumer closed")
			}
			if e = b.accept(session, delivery); e != nil {
				return e
			}
		}
	}
}
func (b *Bridge) publishOne(ctx context.Context, channel *amqp.Channel, returned <-chan amqp.Return) (bool, error) {
	// This is a broker network-operation deadline, not an execution/job deadline.
	work, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tx, e := b.Store.Pool.Begin(work)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(work)
	var id, kind string
	var payload []byte
	var created time.Time
	e = tx.QueryRow(work, `SELECT event_id::text,event_type,payload,created_at FROM `+b.Store.table("outbox")+` WHERE published_at IS NULL ORDER BY created_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id, &kind, &payload, &created)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	body, e := json.Marshal(map[string]any{"event_id": id, "type": kind, "payload": json.RawMessage(payload), "created_at": created})
	if e != nil {
		return false, e
	}
	confirm, e := channel.PublishWithDeferredConfirmWithContext(work, b.EventsExchange, kind, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: id, Type: kind, Timestamp: created, Body: body})
	if e != nil {
		return false, e
	}
	ack, e := confirm.WaitContext(work)
	if e != nil {
		return false, e
	}
	if !ack {
		return false, errors.New("broker publish rejected")
	}
	// RabbitMQ sends Basic.Return before the positive confirm of an unroutable
	// mandatory publication. One publication is outstanding on this channel.
	select {
	case <-returned:
		return false, errors.New("broker publication was not routed; outbox retained")
	default:
	}
	_, e = tx.Exec(work, `UPDATE `+b.Store.table("outbox")+` SET published_at=clock_timestamp(),publish_attempts=publish_attempts+1 WHERE event_id=$1`, id)
	if e != nil {
		return false, e
	}
	return true, tx.Commit(work)
}
func (b *Bridge) accept(ctx context.Context, d amqp.Delivery) error {
	var c Command
	if e := json.Unmarshal(d.Body, &c); e != nil || !validID(c.EventID) {
		if e = b.Store.RejectCommand(ctx, d.Body, "invalid_envelope"); e != nil {
			return e
		}
		return d.Ack(false)
	}
	if d.MessageId != "" && d.MessageId != c.EventID {
		if e := b.Store.RejectCommand(ctx, d.Body, "message_id_mismatch"); e != nil {
			return e
		}
		return d.Ack(false)
	}
	_, e := b.Store.AcceptCommand(ctx, c)
	if errors.Is(e, ErrInvalid) || errors.Is(e, ErrConflict) {
		e = b.Store.RejectCommand(ctx, d.Body, "invalid_or_conflicting_job")
	}
	if e != nil {
		return e
	}
	return d.Ack(false)
}

func (b *Bridge) declare(channel *amqp.Channel) error {
	var e error
	for _, exchange := range []string{b.EventsExchange, b.CommandsExchange} {
		if e = channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); e != nil {
			return e
		}
	}
	for _, name := range []string{b.AuditQueue, b.CommandQueue} {
		if _, e = channel.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum", "x-delivery-limit": int32(-1)}); e != nil {
			return e
		}
	}
	if e = channel.QueueBind(b.AuditQueue, "#", b.EventsExchange, false, nil); e != nil {
		return e
	}
	if e = channel.QueueBind(b.CommandQueue, "job.enqueue", b.CommandsExchange, false, nil); e != nil {
		return e
	}

	return nil
}
