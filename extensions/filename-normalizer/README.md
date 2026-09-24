# File Normalizer

The File Normalizer prepares incoming documents for Paperless-ngx without
changing their contents. It is written in Go and ships as two independently
deployable executables backed by a PostgreSQL cluster and a RabbitMQ broker.

**Status: locally runnable, synthetic only.** Discovery, normalization,
destination reservation, safe publication, completion accounting, dry run,
declarative configuration and a gRPC inspection API are implemented and
exercised end to end against real PostgreSQL, RabbitMQ and filesystems.

The naming policy is a **documented candidate** (`v1-candidate-2026-09-16`),
not an owner-accepted production policy; every surface that reports it says so.
Production filesystem, security, monitoring, backup and deployment
qualification are separate and unfinished.

- [Local stack](../../deploy/filename-normalizer/README.md) — containerized development commands.
- [Deployment package](../../deploy/filename-normalizer/production/README.md) — storage, monitoring, backup and deployment procedures.

## The two applications

| Executable | Role |
|---|---|
| `fn-watcher` | Hosts three workers: **discovery** (registers eligible completed submissions from the incoming root, and reconciles work that arrived while the process was down), **dispatch** (publishes durable jobs with publisher confirms and returns stranded claims), and **completion accounting** (aggregates the ledger into the exposed metrics). |
| `fn-renamer` | Consumes the work queue with manual acknowledgement and bounded concurrency, independently scalable. Owns normalization, destination reservation and publication: verified working copy, exclusive name reservation, publish intent, `link(2)` into the consume directory, durable receipt. |
| `fnctl` | The gRPC inspection client, shipped as its own image. Read-only. |

Both binaries accept four subcommands:

| Subcommand | Purpose |
|---|---|
| `version` | Prints the stamped build identity, including the source digest. |
| `check-config` | Validates the configuration and prints the effective, credential-free view. |
| `healthcheck` | Queries the **running** process over its own HTTP surface. This is the container healthcheck; the runtime images are `FROM scratch` and have no shell, curl or wget. |
| `probe [--require-ready] [--interval D --duration D] [--output FILE] URL...` | Reads `/readyz` from one or more targets, optionally sampling repeatedly into a JSON-lines file. Exits non-zero with `--require-ready` if any target is not ready. |

## What a document's path actually is

```
incoming/                 a producer writes a temporary name and renames it into place
        │
        ▼
watcher / discovery       selection patterns decide eligibility (never transform a name)
        │                 completion contract: stability heuristic (watched for
        │                 the whole interval), or rename
        │                 fingerprint + source identity: root+name+inode, and the
        │                 birth time where the filesystem reports one (the device
        │                 otherwise; it changes on every SMB remount)
        ▼
ledger.RegisterJob        durable job identity (uuid), stamped with the POLICY IDENTITY
        │                 it is accepted under; state pending_dispatch
        ▼
watcher / dispatch        claim (FOR UPDATE SKIP LOCKED) → publish persistent message
        │                 → wait for publisher confirm → record dispatched
        ▼
RabbitMQ                  durable direct exchange → quorum queue → DLX for rejected messages
        │                 payload is an opaque job reference only: no bytes, names, paths or hashes
        ▼
renamer / consumer        manual ack, prefetch = concurrency
        │                 refuse a job whose policy identity is not this process's
        ▼
naming.Normalize          candidate policy; refuses rather than guesses a file type
        │
        ▼
staging/<job>.work        verified working copy, checked against the recorded fingerprint;
        │                 the source is re-inspected afterwards
        ▼
ReserveName               INSERT on a primary key: concurrent workers, one winner
        │
        ▼
RecordPublishIntent       committed BEFORE the link, so a crash is recoverable as
        │                 "may have published" rather than indistinguishable from
        │                 "never started"
        ▼
link(2) into consume/     fails if the name exists; never overwrites; the staged target
        │                 lives in the destination directory so the atomic step never
        │                 crosses a filesystem boundary
        ▼
RecordDelivered           receipt + state delivered, in one transaction,
        │                 committed BEFORE the acknowledgement
        ▼
basic.ack
```

Every branch that cannot reach a receipt ends in a durable, visible outcome
instead: `held` with a closed-set category, or `uncertain` when a publication
may have happened and cannot be confirmed. `uncertain` is never redelivered —
the consumer may already have the document.

In a **dry run** everything from `staging/` onwards is skipped: the name is
computed and recorded, and nothing else is touched.

## Layout

```text
extensions/filename-normalizer/
├── cmd/
│   ├── watcher/            # fn-watcher entry point
│   └── renamer/            # fn-renamer entry point
├── internal/
│   ├── app/                # shared wiring: logger, metrics, ledger, broker, health
│   ├── broker/             # RabbitMQ: topology, publisher confirms, manual-ack consumer
│   ├── buildinfo/          # build identity stamped via -ldflags
│   ├── config/             # environment configuration and validation
│   ├── health/             # /healthz, /readyz, /metrics and the probe loop
│   ├── jobs/              # durable job vocabulary and the versioned message contract
│   ├── ledger/             # PostgreSQL job store, embedded migrations, replication reads
│   ├── logging/            # structured JSON logging with a log-key allow-list
│   ├── renamer/            # the renamer application
│   ├── runtime/            # worker supervision and graceful shutdown
│   ├── storage/            # storage-role probing
│   ├── telemetry/          # Prometheus registry and metric definitions
│   └── watcher/            # the watcher application
├── test/integration/       # containerized integration suite (build tag: integration)
└── Dockerfile              # toolchain, verify, integration and two scratch runtime stages
```

## Building, running and testing — all in containers

Everything happens inside containers. **No host Go installation is required or
used.** The orchestration entry points live next to the compose file:

```sh
cd ../../deploy/filename-normalizer
make help
```

See [the deployment README](../../deploy/filename-normalizer/README.md) for the
full command set, the PostgreSQL topology and the evidence layout.

## Configuration

Every setting comes from the environment. Any secret may instead be supplied
through a `<VAR>_FILE` indirection, so credentials arrive as a mounted file
rather than an inherited environment variable. Validation reports **every**
problem at once rather than one per restart.

### Shared

| Variable | Default | Meaning |
|---|---|---|
| `FN_INSTANCE` | hostname | Instance identity used in logs and in the broker connection name. |
| `FN_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `FN_HTTP_ADDR` | `:8080` | Address for `/healthz`, `/readyz` and `/metrics`. |
| `FN_SHUTDOWN_TIMEOUT` | `20s` | Drain budget after SIGTERM. |

### PostgreSQL

| Variable | Default | Meaning |
|---|---|---|
| `FN_DB_PRIMARY_HOST` | *required* | Primary endpoint. A standby answering here is rejected, not treated as healthy. |
| `FN_DB_PRIMARY_PORT` | `5432` | |
| `FN_DB_REPLICA_HOST` | *(none)* | Standby endpoint, opened as a separate read-only pool. |
| `FN_DB_REPLICA_PORT` | `5432` | |
| `FN_DB_REPLICA_REQUIRED` | `false` | When true, a lost standby makes the application not ready. |
| `FN_DB_NAME`, `FN_DB_USER` | *required* | |
| `FN_DB_PASSWORD` / `FN_DB_PASSWORD_FILE` | *required* | Any valid PostgreSQL password, including one containing spaces, quotes or backslashes: every DSN value is single-quoted with backslash escaping per the libpq keyword/value rules. |
| `FN_DB_SSLMODE` | `disable` | Any libpq sslmode. `disable` is only appropriate on a private container network. |
| `FN_DB_MAX_CONNS` | `8` | |
| `FN_DB_CONNECT_TIMEOUT` | `5s` | |
| `FN_DB_APPLY_MIGRATIONS` | watcher `true`, renamer `false` | Migrations run under an advisory lock, so concurrent watchers serialise rather than race. |

### RabbitMQ

| Variable | Default | Meaning |
|---|---|---|
| `FN_AMQP_HOST` | *required* | |
| `FN_AMQP_PORT` | `5672` | |
| `FN_AMQP_VHOST` | `/` | |
| `FN_AMQP_USER` | *required* | |
| `FN_AMQP_PASSWORD` / `FN_AMQP_PASSWORD_FILE` | *required* | |
| `FN_AMQP_EXCHANGE` | `filename_normalizer.jobs` | Durable direct exchange. |
| `FN_AMQP_QUEUE` | `filename_normalizer.jobs.v1` | Durable quorum queue. |
| `FN_AMQP_ROUTING_KEY` | `normalize` | |
| `FN_AMQP_DLX` | `<exchange>.dlx` | Dead-letter exchange. |
| `FN_AMQP_DEAD_LETTER_QUEUE` | `filename_normalizer.jobs.v1.dead` | |
| `FN_AMQP_DELIVERY_LIMIT` | `5` | Declared as `x-delivery-limit`. See the caveat below. |
| `FN_AMQP_CONFIRM_TIMEOUT` | `10s` | A publication that does not settle within this window is **not** recorded as dispatched. |
| `FN_AMQP_DIAL_TIMEOUT` | `5s` | |
| `FN_AMQP_HEARTBEAT` | `10s` | |
| `FN_AMQP_RECONNECT_DELAY` | `2s` | Also the consumer's detach backoff. |

### Storage roles

| Variable | Default | Watcher | Renamer |
|---|---|---|---|
| `FN_STORAGE_INCOMING` | `/var/lib/filename-normalizer/incoming` | read | read |
| `FN_STORAGE_QUEUED` | `…/queued` | write | write |
| `FN_STORAGE_STAGING` | `…/staging` | write | write |
| `FN_STORAGE_CONSUME` | `…/consume` | **read** | write |
| `FN_STORAGE_FAILED` | `…/failed` | write | write |
| `FN_STORAGE_REQUIRED` | `true` | Gates readiness on the probe | |

The consume role is read-only for the watcher on purpose: publication belongs
to the renamer. The probe expects that split, so mounting consume writable for
the watcher is a deployment error the application will not benefit from.

Each root is **probed**, never merely listed. A root that is missing, is not a
directory, or cannot be read is reported with its own status, distinct from a
directory that is present and simply empty. Probing never creates a root:
silently creating one would mask an incorrect mount.

### Watcher-specific

| Variable | Default | Meaning |
|---|---|---|
| `FN_DISCOVERY_ENABLED` | `false` | Enables the discovery worker. Off unless a deployment declares it: a watcher that starts scanning a root nobody configured is a surprise, not a default. |
| `FN_DISCOVERY_INTERVAL` | `10s` | Scan interval. |
| `FN_DISCOVERY_STABILITY_INTERVAL` | `30s` | How long a submission must hold still — size, modification time and identity unchanged, as watched by the watcher itself — and how old its modification time must be. A heuristic, not proof. The modification time alone is not trusted: over SMB it can stop advancing while a file is still being written. After a restart, a waiting file is registered one interval later. |
| `FN_DISCOVERY_BATCH` | `100` | Registrations per scan. |
| `FN_WATCHER_RECONCILE_ON_START` | `true` | Scan immediately at startup, so work that arrived during an outage is not delayed by a whole interval. |
| `FN_CONFIG_FILE` | *(unset)* | The declarative configuration file. Selection patterns, transform rules and the completion contract live only there. |
| `FN_GRPC_ADDR` | `:9090` | The inspection API's listen address. Empty disables it. Not published to the host, and unauthenticated. |
| `FN_WATCHER_DISPATCH_INTERVAL` | `1s` | |
| `FN_WATCHER_DISPATCH_BATCH` | `32` | |
| `FN_WATCHER_DISPATCH_CLAIM_MAX_AGE` | `60s` | After this, a stranded dispatch claim is returned to `pending_dispatch`. |
| `FN_WATCHER_ACCOUNTING_INTERVAL` | `5s` | |

### Renamer-specific

| Variable | Default | Meaning |
|---|---|---|
| `FN_RENAMER_CONCURRENCY` | `1` | Bounded simultaneous in-flight deliveries per instance (1–64). |
| `FN_RENAMER_PREFETCH` | = concurrency | Must be at least the concurrency bound. |
| `FN_RENAMER_MAX_DELIVERY_ATTEMPTS` | `5` | Bounds redelivery. Counted from the ledger's own durable counter, not from the broker message, which does not advance on a requeue. |
| `FN_RENAMER_DRY_RUN` | `false` | Compute and record names; touch nothing. |

### Refused on purpose

`FN_CLEANUP_ENABLED=true` is a startup error. Source deletion and ledger
purging are unresolved policy and are not implemented; accepting the flag
would imply a behaviour that does not exist.

## Telemetry

**Logs** are JSON on stdout. The `logging` package enforces an allow-list of
log keys: anything outside it is replaced with a redaction marker, so a caller
cannot emit a document name, path, fingerprint or credential even by accident.
Errors are logged as a coarse `error_kind` classification rather than as error
text, because driver and filesystem error strings routinely embed a DSN or a
path. Opaque job IDs **are** permitted in logs.

**Metrics** are Prometheus text on `/metrics`. Every label is drawn from a
closed set enumerated in code, and the whole label space is pre-initialised at
zero so a scrape can tell "nothing happened yet" from "this series does not
exist". Job IDs and document names are never label values.

Key series: `fn_build_info` (including a `source_digest` label),
`fn_ready`, `fn_dependency_up{dependency}`,
`fn_storage_root_available{role}`, `fn_storage_root_status{role,status}`,
`fn_ledger_schema_usable`, `fn_ledger_schema_version_installed`,
`fn_ledger_schema_version_expected`, `fn_postgres_replica_in_recovery`,
`fn_jobs{state}`, `fn_oldest_pending_dispatch_age_seconds`,
`fn_dispatch_attempts_total{outcome}`, `fn_dispatch_confirmed_total`,
`fn_dispatch_reclaimed_total`, `fn_deliveries_total{outcome}`,
`fn_redeliveries_total`, `fn_deliveries_in_flight`,
`fn_delivery_duration_seconds`, `fn_consumer_up`,
`fn_renamer_concurrency_limit`, `fn_renamer_prefetch_limit`,
`fn_broker_reconnects_total`, `fn_ledger_errors_total{error_kind}`.

The two `fn_renamer_*_limit` gauges publish this instance's configured bounds
(0 on the watcher, which hosts no consumer). They exist so that an observer
checking "in-flight work is within its bound" can read the bound from the
instance it is checking, rather than from whatever configuration the observer
happens to have loaded.

**Readiness** at `/readyz` reports each dependency with a sanitized category
and returns 503 while any required dependency is unusable. It is truthful by
construction: the states come from real probes, not from a startup flag. Three
properties are worth stating explicitly.

*Connectivity is not readiness.* The database check also requires the ledger
schema to be **usable** — present, and at least the migration version this
binary expects. A reachable database whose migration never ran or was blocked
would otherwise let an instance advertise itself as ready and then fail on its
first durable write. The renamer's consumer is gated on the same condition, so
it never takes a delivery it could not settle.

*A passing check may still carry a category.* A promoted standby answers
queries, so its check succeeds — and `promoted_not_in_recovery` is exactly the
thing an operator needs to see. Categories are therefore retained on success
rather than cleared.

*The drain withdrawal is latched.* Once shutdown begins, readiness is off for
the rest of the process lifetime. A probe pass already running concurrently
cannot flip it back on, so an observer draining the instance never sees it
advertise itself as available again.

## Shutdown order

Shutdown runs in three phases, and the order is the point:

1. **Readiness is withdrawn** and latched off. Nothing has been cancelled yet,
   so the withdrawal is observable while the instance is still draining.
2. **Service workers are cancelled and waited for.** A consumer stops taking
   new deliveries and lets the ones it already holds finish. In-flight handlers
   run on a context derived with `context.WithoutCancel`, so shutdown stops new
   work without aborting the durable write of work already accepted; a bounded
   handler budget keeps that from becoming an unbounded wait.
3. **Infrastructure workers are cancelled last.** The broker connection and the
   database pools therefore stay usable for the whole drain. Cancelling one
   shared context instead would close the broker connection underneath a
   consumer that was still settling a delivery — which is precisely the bug this
   ordering exists to prevent.

## Three behaviours worth knowing before you read the code

**A publisher confirm can arrive after the renamer has already settled the
job.** The dispatcher claims a row, publishes, and records the confirmation.
A renamer can consume and complete that job before the confirmation write
lands. `MarkDispatched` therefore records the dispatch timestamp and history
row unconditionally and only advances the state if nothing else has moved it
on — otherwise a job that was demonstrably published would end up looking as
though it never was.

**A broker close can land between acquiring the publishing channel and using
it.** The publisher drains stale unroutable returns before each publication.
The AMQP client closes that notification channel whenever the connection or
channel closes, and a closed Go channel is permanently ready to receive — so a
drain loop that ignores the second receive value spins instead of terminating.
Because the dispatch worker also performs stranded-claim recovery, a publisher
stuck there would stop claim recovery too, and reconnecting the broker would
not restore progress. The drain therefore stops on a closed channel and the
publication is reported as not dispatched; the job stays pending and is
republished under the same identity. A confirmed publication whose routability
can no longer be observed is likewise reported as unconfirmed rather than
assumed routable.

**On RabbitMQ 4.3.6 an explicit `basic.nack(requeue=true)` does not advance a
quorum queue's `x-delivery-count`.** The declared `x-delivery-limit` therefore
cannot bound a consumer that keeps requeueing, even though it does bound
redelivery after a connection loss. This is measured by the integration suite,
not assumed. Consequently the renamer **stops consuming** when it cannot
durably settle a delivery: it returns the message, cancels its consumer, and
waits out a backoff before reattaching. Without that, a database outage would
produce thousands of requeues per second. The suite asserts the bound holds
(two requeue decisions across a 25-second outage, with both consumers
detached and the message still queued).
