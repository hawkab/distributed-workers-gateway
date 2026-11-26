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
- `WORKER_ARTIFACT_DIR`: private persistent filesystem for streamed artifacts.
  Without it, legacy JSON jobs work and artifact writes return 503. The service
  user needs write access; the directory is never an nginx document root.

The reverse proxy must strip a configured base prefix, e.g. `/fleet`, while
preserving the relative routes documented in [the protocol contract](https://gitflic.ru/project/distributed-workers/protocol).
No job execution is tied to the lifetime of an HTTP request. Device requests
use a 16 MiB JSON control budget. Binary chunk routes stream directly to private
storage with a fixed copy buffer and no aggregate artifact-size deadline or cap.

## Worker identity and resource admission

An administrator creates an enrollment credential with
`POST /v1/admin/enrollments {}`. Its hash is stored without a worker identity
until the device calls `POST /v1/workers/connect {}`. Connect requires that
credential and `X-Worker-Profile`: unpadded base64url of the protocol's schema 1
JSON profile. The server atomically assigns a UUID. Retrying the same credential,
including after losing the reply or restarting either side, returns the same ID;
the bearer credential remains unchanged. Existing device tokens retain their
legacy IDs. There is no anonymous registration or client-selected new ID.

Profiles contain capability names and numeric CPU/RAM/disk/GPU availability only.
Unknown values remain unknown; model names, hostnames and serials are rejected.
`GET /v1/workers/me` exposes only the caller's stored profile and server observation
time. `GET /v1/jobs/current` returns only its own live assignment, without extending
the lease. Fleet/history views require the separate administrator credential.

Heartbeat and claim accept an optional profile header for legacy compatibility.
When present, its capabilities are authoritative; conflicting body capabilities
return 400. New jobs can declare `resource_requirements` for available CPU cores,
RAM, disk, GPU count, per-GPU compute units and allocatable GPU memory. Positive
requirements need a fresh profile on that claim RPC; stored old availability is
never reused. Ordinary jobs remain available to legacy clients. Eligibility is
filtered in SQL before the queue limit/row locks, so unsuitable jobs do not hide
a later compatible job. Dedicated GPUs are preferred over shared-memory devices;
unified or unknown-sharing GPU memory also consumes the available host-RAM budget.
GPU total memory alone does not prove an allocation budget.

## Streamed results and external effects

Create `/v1/jobs/{job}/artifacts` with the attempt identity, stable `key`, optional
metadata-only `name`, and `media_type`. PUT raw chunks to
`/v1/artifacts/{upload_id}/chunks/{offset}` with the device bearer token and
`X-Worker-Id`, `X-Attempt-Id`, `X-Fencing-Token`. An ACK advances `next_offset` only
after file fsync and PostgreSQL commit. Status returns the durable `offset`,
`last_segment` and `segments_count`; it does not buffer an unbounded manifest.
Finish with the exact `length` and optional expected `sha256`. The receipt includes
`artifact_id`, `name`, `sha256`, `length`, `media_type`, `committed` and
`candidate_saved`. This seals storage, not the job result. `Complete.artifact_ids`
is checked against sealed artifacts of the same job and the current job fence.

One-chunk results are hard-linked into content-addressed storage. Multi-chunk
results are merged with bounded memory. Redundant segments are reclaimed only
after the complete object and committed PG record are verified. Reconciliation
recovers interrupted cleanup; duplicate chunk/finish requests return stable ACKs.
Aborted streams do not publish partial segments. An explicit abort retains already
acknowledged segments. Stale attempt outputs remain candidates, and a current
attempt may explicitly adopt a sealed earlier-attempt output of the same job.

`GET /v1/admin/artifacts/{id}/content` requires the admin token. The worker content
route requires device/attempt headers and a current live attempt of the same job.
Artifact/effect status and cached begin-response replay have the same restriction;
the former creator loses read access after expiry, reassignment or completion.
Late write receipts and candidate outputs are still retained. Neither route
exposes filesystem paths.
Keep PG metadata and artifact storage together when backing up or restoring.
The filesystem must support atomic hard links and fsync; no expiration deletes
candidate or canonical objects implicitly.

HTTP effect intents use `/v1/jobs/{job}/effects/{effect_id}/begin` and `/finish`.
Begin receives an execution UUID and the original request object; the server
normalizes defaults and headers and returns its request SHA-256. Retrying a lost
begin RPC with the same live execution UUID preserves the execution grant. A new
execution cannot repeat an unresolved `never` mutation; it receives `uncertain`.
Completed responses are replayed across attempts. `safe` is limited to GET/HEAD;
`idempotent` requires a matching vendor idempotency header and key. Finish checks
the reserved execution and hash, preserves late candidates and returns a stable
receipt. HTTP status/body interpretation remains a worker operation.

RabbitMQ receives small artifact/effect/result references. Binary bodies, large
response metadata and attachment lists stay in file storage/PostgreSQL. Reverse
proxies must disable request/response buffering for binary routes and remove
request-size caps there. JSON controls retain their separate budget. The Go
listener has header/idle protection but no overall read/write deadline, and
graceful shutdown drains active requests rather than imposing a fixed deadline.

## Pilot operations

`fleetctl` reads `WORKER_API_URL` (default `http://127.0.0.1:18220`) and
`WORKER_ADMIN_TOKEN` from its environment. Commands:

- `fleetctl init-worker`: creates a pending enrollment credential; the device
  obtains its server ID through connect. Supplying `DEVICE-ID` preserves the
  legacy administrator registration flow. Capture credentials only into private
  provisioning storage, never logs.
- `fleetctl enqueue job.json`: submits `{operation,input,script?,required_capabilities?,resource_requirements?,idempotency_key?}`.
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

Queue consumers can send `Prefer: wait=20` to `/v1/jobs/claim` or `/v1/admin/jobs/wait`. Committed PostgreSQL notifications wake waiting requests; idle renewal windows do not limit running computations. Admin consumers should also send `return=minimal` to receive terminal IDs and result references, then read `/v1/admin/jobs/{id}/result` once. This avoids aggregating large results or repeatedly transferring job inputs and history. Binary artifacts remain streamed.

### Adaptive execution slots

Claims support `slot_id`, defaulting to zero. An optional profile `execution`
budget controls simultaneous admission without changing the worker identity.
Repeating a claim recovers only that slot's live attempt. Zero pauses new
admission and does not cancel a lease. Legacy profiles retain single-slot
behavior. Startup adds the slot column and live-slot index to the dedicated
worker schema; all existing attempts migrate to slot zero.
