# File Normalizer local foundation stack

An isolated, container-only stack for developing and exercising the
[File Normalizer](../../extensions/filename-normalizer/README.md): both
applications, a replicated PostgreSQL cluster, a RabbitMQ node, synthetic
document fixtures, and the containerized integration suite.

Everything runs in containers. **No host Go installation is required or used**
for dependency resolution, formatting, vetting, building, testing or running.
The host is used only to edit files, inspect git, and orchestrate containers.

This stack is for local synthetic work. It connects to nothing external, holds
no real documents, and its credentials are throwaway values generated on the
machine that runs it.

## Quick start

```sh
cd deploy/filename-normalizer

make env       # generate .env and secrets/ with local-only credentials
make verify    # gofmt, go vet and unit tests, inside the toolchain image
make build     # build every image in the stack
make up        # start the cluster, broker and applications; print readiness
make test      # run the full phased integration suite (F1-F6)
make evidence  # collect logs, readiness, metrics and real cluster/broker state
make down      # stop, keeping volumes and data
```

`make help` lists every target.

### Copyable command set

```sh
# ---- setup (once) ----
cd deploy/filename-normalizer
make env

# ---- build ----
make verify
make build

# ---- start ----
make up
make status
make ready

# ---- test ----
make test                 # every phase, F1-F6
make test-baseline        # steady-state phase only
make test-failover        # manual standby promotion, then rebuild the standby
make test-project-isolation    # prove the guarded removal targets only its own
                               # project's volume
make test-failure-propagation  # prove make verify and run-logged.sh propagate
                               # a containerized failure

# `make test` does NOT include the three exercises above. The first two are
# destructive to a standby's data volume, and the third deliberately breaks the
# tree for one step, so all three are run explicitly and their evidence is
# retained alongside the suite's.

# ---- observe (all from inside the project network) ----
make logs                 # follow application logs
make ready                # readiness of all three; EXITS NON-ZERO if any is not ready
make evidence             # snapshot into extensions/filename-normalizer/.evidence/

# ---- stop ----
make down                 # keep data
make down-clean           # remove this project's containers, network and volumes
make clean-evidence       # remove this project's collected evidence
```

## Isolation

Every container, network and volume is prefixed with `COMPOSE_PROJECT_NAME`
(default `fnfoundation`), so `make down-clean` removes only this project's
resources and leaves unrelated containers, volumes and development data
untouched. Document storage lives in named volumes, so nothing is written into
your working tree except the gitignored `.evidence/` directory.

No database or broker port is published to the host. Only the applications'
HTTP surfaces are, on ports 18080–18082, so readiness and metrics can be read
from outside the network.

## Services

| Service | Image | Notes |
|---|---|---|
| `postgres-primary` | `postgres:17.11-alpine` (digest-pinned) | Primary. Healthy means "accepting connections **and** not in recovery". |
| `postgres-replica` | same image | Asynchronous streaming standby. Healthy means "answering **while still in recovery**". |
| `rabbitmq` | `rabbitmq:4.3.6-management-alpine` (digest-pinned) | Single node. Durable topology, quorum queues, persistent messages. |
| `storage-init` | postgres image (reused; adds no new dependency) | One-shot. Hands the storage volumes to the unprivileged runtime account and generates the synthetic document fixtures. The only container that needs root. |
| `watcher` | built here | One instance. |
| `renamer-1`, `renamer-2` | built here | Two independent instances, declared as separate services rather than replicas so each has a stable identity and its own readiness endpoint. |
| `integration` | built here, profile `test` | The integration suite, compiled from the same source tree and linked against the same packages as the applications. |
| `watcher-storage-fault` | built here, profile `fault` | A watcher whose incoming root was never mounted. Used to show that unavailable storage produces a truthful not-ready application. |
| `renamer-no-schema` | built here, profile `fault` | A renamer pointed at a reachable database that has no ledger schema. Used to show that connectivity is not readiness. |
| `readiness-probe` | built here, profile `test` | Runs the renamer binary's `probe` subcommand and mounts the evidence directory, so readiness can be sampled to a file during a drain. |

### Inspecting a running stack

Everything goes through containers on the project network:

```sh
make ready       # readiness JSON for all three applications; non-zero exit if any is not ready
make status      # container state
make logs        # follow application logs
make evidence    # full snapshot: readiness, metrics, cluster and broker state, logs
```

`make ready` runs the application binary's own `probe --require-ready`
subcommand. The binaries expose two subcommands for this, because the runtime
images are `FROM scratch` and contain no shell, curl or wget:

| Subcommand | Purpose |
|---|---|
| `healthcheck` | Asks the **running** process for its liveness document over the HTTP surface that process is serving. This is the container healthcheck. It replaced a `version` check, which only proved that a fresh copy of the binary could start and would have reported a wedged or dead server as healthy. |
| `probe [--require-ready] [--interval D --duration D] [--output FILE] URL...` | Reads `/readyz` from one or more targets, optionally sampling repeatedly and appending JSON-line observations to a file. With `--require-ready` it exits non-zero unless every target's last observation is ready. |

The container healthcheck is deliberately **liveness, not readiness**: a
dependency outage must leave the container running and reporting not-ready,
rather than being treated as a dead container.

### What readiness actually requires

`/readyz` returns 503 while any required dependency is unusable, and each
check reports a sanitized category. A check may report a category **even when
it passes**, because some conditions pass while still needing an operator's
attention — a promoted standby is the case in point: it answers queries, so
the check succeeds, and the category `promoted_not_in_recovery` is what tells
you it is no longer replicating.

The database check requires more than connectivity: it also requires the
**ledger schema to be usable** — present, and at least the migration version
the binary expects. A reachable database whose migration never ran would
otherwise let an instance advertise itself as ready and then fail on its first
durable write. The renamer's consumer is gated on the same condition, so it
will not take deliveries it cannot settle. `fn_ledger_schema_usable`,
`fn_ledger_schema_version_installed` and `fn_ledger_schema_version_expected`
expose this to a scraper.

### Application container hardening

The application containers run `user: 65532:65532`, `read_only: true`,
`cap_drop: [ALL]` and `no-new-privileges`. The runtime images are built
`FROM scratch` and contain one static binary, a passwd/group pair for the
non-root account, CA certificates and zoneinfo — no shell and no package
manager. Nothing is downloaded at container start: every dependency is
resolved and compiled at image build time. `make evidence` records the
observed hardening in `container-hardening.txt`.

Third-party images run with their own defaults; the hardening statement above
is about the two applications.

## PostgreSQL topology

**One primary with one asynchronous streaming standby, using a physical
replication slot.**

```
┌──────────────────┐   streaming WAL over the project network   ┌──────────────────┐
│ postgres-primary │ ─────────────────────────────────────────► │ postgres-replica │
│  wal_level=      │        slot: fn_standby_1                  │  hot standby     │
│    replica       │        sync_state: async                   │  read-only       │
│  reads + writes  │ ◄──── hot_standby_feedback ─────────────── │  pg_is_in_       │
│                  │                                            │   recovery()= t  │
└──────────────────┘                                            └──────────────────┘
        ▲                                                                ▲
        │ writes and reads                                               │ observed by the
        │ (FN_DB_PRIMARY_HOST)                                           │ applications'
        └──────────── watcher, renamer-1, renamer-2 ─────────────────────┘ replica pool
                                                                (FN_DB_REPLICA_HOST)
```

Both applications open the standby as a **separate read-only pool**, so they
can observe replication rather than assume it. The standby appears as its own
readiness check, and a standby that has left recovery — meaning it was
promoted — is reported as such rather than silently passing.

### Why asynchronous

A synchronous standby would make every commit depend on the standby being up,
which is the wrong trade-off for a two-node development cluster: stopping the
standby would stall the applications instead of demonstrating degraded
operation. The cost is stated plainly below.

### Supported recovery behaviour

Exercised by `make test-failover`, with evidence written to
`.evidence/failover-*`:

- **Replication works.** A row written on the primary becomes visible on the
  standby (measured at ~4 ms in a local run), `pg_stat_replication` shows a
  walsender in `streaming`/`async` state, and the standby rejects writes.
- **Restart persistence** of the primary. An orderly `compose restart` sends
  SIGTERM, so PostgreSQL shuts down cleanly and comes back with no recovery
  work to do. Rows and their retained history survive it, and the applications
  recover readiness on their own without a restart. This establishes
  persistence across a restart — **not** crash recovery.
- **Crash recovery of the primary**, established separately by a SIGKILL to the
  primary container, which kills the postmaster outright. `fsync`,
  `synchronous_commit` and `full_page_writes` are all on, so PostgreSQL then
  performs WAL recovery on startup. The assertion requires PostgreSQL's own log
  to report the improper shutdown, so the phase cannot be satisfied by an
  orderly restart.
- **Standby outage and catch-up.** The replication slot makes the primary
  retain WAL the standby has not consumed, so a standby that is down for a
  while catches up instead of needing a rebuild. The trade-off: a permanently
  dead standby would accumulate WAL on the primary.
- **Manual promotion.** `pg_ctl promote` on the standby is exercised: the
  promoted node leaves recovery, still holds the replicated data, and accepts
  writes.

### What is **not** supported

- **There is no automatic failover.** Nothing watches the primary and nothing
  repoints the applications. `FN_DB_PRIMARY_HOST` is static, so a promoted
  standby only serves the applications after an operator changes that
  configuration. This has not been automated and is not claimed to work.
- **Promotion is not lossless.** Because replication is asynchronous, a
  promotion can lose commits the primary had not yet shipped. The failover
  exercise proves the replicated data survived; it does not prove zero data
  loss.
- **A promoted standby cannot rejoin as a standby.** The failover script
  therefore discards the diverged standby volume and takes a fresh base
  backup. The volume is identified from the running standby container's own
  mounts and then verified against its Compose project and volume labels
  before removal, so a name that merely looks right — or a same-named volume
  belonging to another project — is refused rather than deleted. Assembling
  the name from a project string read out of `.env` was not safe: a shell
  `COMPOSE_PROJECT_NAME` takes precedence over `.env` for every Compose
  command, so a destructive step keyed off the `.env` value could target a
  different project's retained volume.

  `make test-project-isolation` demonstrates this end to end, and it is
  deliberately hard to satisfy: it refuses to run if the probe project already
  has any container or volume (it will not destroy what it did not create, and
  installs no cleanup until that check passes), starts the probe project as a
  **full** stack so the nested exercise can actually complete, requires the
  nested exit status to be zero, requires the nested log to show the guard
  removing the probe's *own* volume with a changed creation timestamp, calls
  the real guard against another project's volume and requires a refusal, and
  requires the unrelated retained volume to survive with an unchanged creation
  timestamp.
- This is a development topology. It establishes nothing about a production
  cluster, its sizing, its backup strategy or its failover automation.

## RabbitMQ

A single node. **Clustering is deliberately out of scope for this increment**
and is not an imposed requirement. Durability comes from a durable direct
exchange, quorum queues, persistent messages, publisher confirms and manual
consumer acknowledgements — all of which survive a restart of the node, which
the integration suite verifies by leaving persistent messages queued across a
broker restart.

A stable `hostname` keeps the node name stable, which is what lets the
persisted quorum-queue state survive a container restart.

### The delivery-limit caveat

The work queue declares `x-delivery-limit` (default 5) and a dead-letter
exchange. The suite proves the limit is genuinely in effect by redeclaring the
queue with a different value and requiring the broker to refuse.

However, **on RabbitMQ 4.3.6 an explicit `basic.nack(requeue=true)` does not
advance a quorum queue's `x-delivery-count`** — measured, not assumed, in
`.evidence/baseline/f4-requeue-counter-behaviour.txt`. The declared limit
therefore cannot bound a consumer that keeps requeueing, although it does
bound redelivery after a connection loss. The renamer consequently stops
consuming when it cannot durably settle a delivery, rather than relying on the
broker to break the loop.

## Credentials

`make env` writes two things, both gitignored:

- `.env` (mode 0600) — values the stock PostgreSQL and RabbitMQ images need as
  environment variables, plus the stack's tunables.
- `secrets/` (directory mode 0700, files mode 0644) — the password files the
  applications read through their `FN_*_PASSWORD_FILE` indirection.

Compose bind-mounts each secret file and **ignores** the `uid`, `gid` and
`mode` keys outside Swarm, so the host file's own mode is what the container
sees. The files are therefore 0644 inside a 0700 directory: the directory keeps
them private on the host, while the file mode lets the containers'
unprivileged account read the mount, which resolves the path without
traversing the directory.

These credentials exist only for this isolated local stack. Nothing here may be
reused for a real system, and no credential from an existing system belongs in
this stack.

## Synthetic fixtures

`storage-init` generates the document fixtures from `/dev/urandom` on every
start, into `incoming/synthetic/`. **No real document is ever used.** The names
deliberately cover the shapes the naming policy will have to handle —
whitespace, mixed case, punctuation, German characters, CJK — plus a
`.part` temporary suffix and a hidden file that discovery must skip. They are
inputs for a later milestone; nothing in this build reads or renames them.

The consume root stays empty, and the integration suite asserts that: nothing
in this increment publishes a document, so a file appearing there would mean
something fabricated a delivery.

## The integration suite

`make test` runs `scripts/run-integration.sh`, which walks the stack through a
sequence of real, isolated states and runs the containerized suite against
each one:

| Phase | Stack state | Covers |
|---|---|---|
| `baseline` | everything healthy | F1 (incl. source provenance), F2, F3, F4, F6, F7 |
| `telemetry` | unchanged, logs re-collected; a slow database statement is forced | F6 log-content assertions |
| `stack_privacy` | unchanged, every stack log re-collected | F6 privacy across application, database and broker logs |
| `replica_down` | standby stopped | F5 |
| `replica_recovered` | standby restarted | F3, F5 |
| `primary_restarted` | primary restarted with SIGTERM (orderly) | F3 restart persistence |
| `primary_killed` | primary killed with SIGKILL | F3 crash recovery |
| `primary_down` | primary stopped | F5 |
| `primary_recovered` | primary restarted | F3, F5 |
| `rabbit_down` | broker stopped | F4, F5 |
| `rabbit_restarted` | broker restarted | F4 durability, F5 |
| `rabbit_recovered` | broker up | F5 recovery |
| `broker_torn` | broker stopped and started three times under the running watcher | F4 publication under connection loss; dispatch and claim recovery resume |
| `watcher_drained` | watcher terminated with SIGTERM after those interruptions | F2 bounded shutdown |
| `storage_fault` | extra watcher with an unmounted root | F5, F6 |
| `schema_fault` | extra renamer on a reachable database with no ledger schema | F4 readiness |
| `drain_under_load` | queue filled with the consumers stopped | F5 setup |
| `drained` | renamer-2 terminated with SIGTERM **mid-backlog**, readiness sampled throughout | F2 and F5 graceful drain |
| `final_privacy` | unchanged, every stack log re-collected after every fault | F6 privacy across the whole run |

The privacy scan runs twice on purpose. The mid-run `stack_privacy` pass owns
the forced slow-registration positive control; the `final_privacy` pass runs
last, over logs that now contain every outage, recovery, kill and termination in
the run, and must show it covered them — it requires evidence of a primary
outage, a broker outage, an application shutdown, a readiness withdrawal and
PostgreSQL's crash recovery inside what it scanned, so it cannot be the mid-run
scan repeated.

The crash phase requires its own fault to have taken effect: the orchestrator
fails if the SIGKILL command fails and requires the container's exit code to be
137, and the assertion filters PostgreSQL's log by the fault instant so an
earlier restart's recovery records cannot satisfy it.

Each destructive step records the instant the fault was injected, and the log
assertions for that fault read only records from its own interval. The
collected logs hold the whole run, so an error produced by an earlier phase
must not be able to satisfy a later phase's claim.

Two properties of this harness matter:

**No mocks, stubs, in-memory substitutes or fake services.** Every dependency
is the real one, and the suite imports the applications' own packages, so what
it exercises is the integration code that ships. Where a test needs precise
control over acknowledgement, it declares a durable topology named after the
run and removes exactly those queues and exchanges afterwards.

**The oracle has to be able to detect what the claim asserts.** Two examples
that were corrected: the prefetch window is measured from RabbitMQ's own
`messages_unacknowledged` via the management API, not from a counter inside
the consumer — a local counter measures running handlers, which the consumer's
own semaphore already bounds, so it could not establish anything about the
AMQP window. And the concurrency bound is sampled *while* a batch is draining,
with the test requiring that it observed the instance genuinely in flight,
because sampling after a batch drains reads zero and passes whatever the bound
was. A metric that is missing entirely fails an assertion rather than being
read as zero.

**Provenance.** The suite requires every application it tests to report the
same source digest the suite binary was stamped with. The digest is a content
hash of the build context computed during the image build, so a stale
application image fails the run instead of being tested silently. A matching
Go version and a shared git revision label would not have excluded that: an
uncommitted edit leaves the revision unchanged.

**No silent skipping.** Each phase declares which dependencies it has
deliberately interrupted. The suite fails if a required dependency is absent
**and** fails if a dependency that was supposed to be interrupted is still
answering — because in that case the fault never took effect and any
conclusion drawn from the phase would be false. The orchestrator additionally
fails a phase whose output contains no test results, so a suite that exits
before running anything cannot be reported as a pass.

Tests that do not apply to the active phase report the active phase and the
phases they do run in, so an omission is visible in the output rather than
invisible.

### Evidence

Everything lands in `extensions/filename-normalizer/.evidence/` (gitignored):

```text
.evidence/
├── run-metadata.txt          # run id, project, start and finish
├── phase-results.txt         # one line per phase
├── phase-<name>.log          # full suite output per phase
├── logs/<service>.log        # collected container logs, read by the suite
├── <phase>/*.txt|json        # per-assertion artefacts
├── snapshot-<timestamp>/     # readiness, metrics, cluster and broker state
└── failover-<timestamp>/     # manual promotion exercise
```

## Files here

```text
deploy/filename-normalizer/
├── docker-compose.yml        # the whole stack
├── Makefile                  # container-only entry points
├── .env.example              # copy to .env, or run `make env`
├── postgres/primary/         # primary config and the replication init script
├── postgres/replica/         # standby bootstrap (pg_basebackup on first start)
├── rabbitmq/                 # broker config and enabled plugins
└── scripts/
    ├── lib.sh                # shared orchestration helpers
    ├── storage-init.sh       # volume ownership and synthetic fixtures
    ├── run-integration.sh    # the phased suite
    ├── run-failover.sh       # manual standby promotion
    └── collect-evidence.sh   # logs, readiness, metrics, cluster and broker state
```

## Scope boundary

This stack demonstrates local synthetic behaviour only. It establishes nothing
about NAS or SMB mounts, about real Paperless-ngx ingestion, or about any
production environment. There is no Paperless container here, real or
otherwise: any later test claiming Paperless ingestion behaviour must use a
real isolated Paperless instance, or remain explicitly unrun.
