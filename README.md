# Fleet gateway

A small HTTPS-facing gateway behind a reverse proxy. Devices use outgoing HTTPS;
PostgreSQL and RabbitMQ stay private on the dedicated fleet server. This process
never connects to the existing Agent Factory queues or its workflow database.
`worker_fleet` tables in the fleet database are the canonical execution ledger.
RabbitMQ carries durable commands and lifecycle/result-reference events.

## Build and run

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o worker-gateway ./cmd/worker-gateway
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o fleetctl ./cmd/fleetctl
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o fleet-relay ./cmd/fleet-relay
```

Required environment:

- `DATABASE_URL`: connection to the **new fleet database**. Runtime role needs
  CONNECT and ownership/USAGE/CREATE within its precreated `worker_fleet` schema,
  rather than CREATE for the whole database. Init creates the schema only when
  absent, and never needs superuser. Migrations must own the fleet tables.
- `WORKER_ADMIN_TOKEN`: a separate random administrator credential.
- `WORKER_BIND`: default `127.0.0.1:18220`.
- `WORKER_RELEASE_DIR`: directory containing `android-stable/manifest.json` and
  versioned APK files. Keep release-signing private keys outside this directory
  and outside the gateway server.
- `WORKER_AMQP_URL`: optional dedicated fleet broker/vhost connection. When set,
  gateway runs the real outbox publisher and command consumer. When using the
  standalone `fleet-relay`, provide this variable to the relay, not the gateway.

The reverse proxy must strip a configured base prefix, e.g. `/fleet`, while
preserving the relative routes documented in [the protocol contract](https://gitflic.ru/project/distributed-workers/protocol).
No job execution is tied to the lifetime of an HTTP request. Device requests
contain only metadata/JSON; current body budget is 16 MiB (APK downloads are
streamed separately). Large future inputs/results should use artifact references.

## Pilot operations

`fleetctl` reads `WORKER_API_URL` (default `http://127.0.0.1:18220`) and
`WORKER_ADMIN_TOKEN` from its environment. Commands:

- `fleetctl init-worker DEVICE-ID`: enrolls a device and prints its one-time token
  response. Capture it only into private provisioning storage; never into logs.
- `fleetctl enqueue job.json`: submits `{operation,input,script?,required_capabilities?,idempotency_key?}`.
- `fleetctl status [JOB-ID]`, `workers`, `metrics`, `cancel JOB-ID`.

`POST /v1/admin/workers/{id}/policy` or `PATCH /v1/admin/workers/{id}` accepts
`{"update_poll_seconds":30}` for a controlled update test. Set 900 afterwards.
Device heartbeat returns the current value from PostgreSQL, without a restart.

Operations and result JSON are opaque to the gateway; the worker executes them.
Default required capability is the operation name (`sha256`, `text.normalize`,
`javascript`). Explicit capability constraints can target a specialized pool.
A claim is idempotent while that worker owns an active lease, limiting the
pilot to one concurrent job per device.

## Reliability

A lease lasts 120 seconds; job heartbeats renew it without a maximum job age.
PostgreSQL wall-clock checks run after acquiring locks. Lost workers cause a
new attempt with a greater fencing token. Superseded/expired completions are
saved as candidates before `candidate_saved` is returned. Same completion
returns the same receipt; changed content for the same attempt returns 409.
Canonical results, attempts and candidates remain stored. An explicit worker
`failed` result is a business outcome, not retry-budget exhaustion.
Explicit cancellation is reconciled after the worker lease expires, even when
that worker never reconnects. Healthy uncancelled jobs are not reaped by age.

Outbox events are inserted in the same PostgreSQL transaction as job state.
The publisher retires them only after a positive broker confirm **and** no
`Basic.Return`. Uncertain sends can repeat the stable event ID.
Incoming commands commit the inbox and job before a manual broker ACK.
Both quorum queues have `x-delivery-limit=-1`; reconnecting during a database
outage cannot discard a command at RabbitMQ's default poison-message limit.
This single-server pilot is not a replicated HA deployment.

Dedicated broker topology:

- Topic exchanges `fleet.events` and `fleet.commands`.
- Durable quorum queues `fleet.events.audit` (all lifecycle events) and
  `fleet.commands.gateway` (routing key `job.enqueue`).
- Command envelope: `{"event_id":"unique-id","type":"job.enqueue","payload":{"operation":"sha256","input":{"text":"hello"}}}`.
- Result lifecycle events contain `job_id`, `attempt_id`, `status`, `committed`
  and `result_ref`. The referenced result lives in PostgreSQL and is available
  through the admin job endpoint. Invalid commands and conflicting event IDs
  are retained for inspection, not silently dropped.

These are isolated fleet queues. No adapter consumes existing Agent Factory
fixed-operation queues automatically; any later integration needs an explicit
capability mapping and separately authorized publisher.

## Verification

```sh
REGISTRY_TEST_DATABASE_URL=... go test -race -count=1 -v ./...
REGISTRY_TEST_DATABASE_URL=... FLEET_TEST_AMQP_URL=... go test -count=1 -v -run '^TestBroker' .
```

PostgreSQL fixtures use fresh schemas and remove them afterward. The optional
role-isolation test also needs CREATE ROLE in the test environment; runtime does
not. Broker tests use unique `fleet.test.*` objects in the dedicated test/fleet
vhost and clean up only those objects. They verify an unroutable positive
confirm without losing the outbox, 25 real unacknowledged redeliveries, command
idempotence, actual SHA-256 execution and persistent result-event publication.
Do not pass connection secrets on the command line or record them in test logs.
