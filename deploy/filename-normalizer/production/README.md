# File Normalizer — deployment package

A reviewable first deployment. It deploys the two applications against a
PostgreSQL primary and a RabbitMQ you already run, with the storage roles bound
to real directories.

**This package has not been deployed anywhere.** It has been validated locally
against the real dependencies (`make -C .. verify-deployment-package`). Target
qualification — the actual filesystem, the actual Paperless, NAS/SMB semantics —
has NOT happened and cannot be inferred from the local stack. See
"Target qualification" below.

---

## 1. What is running

Every process reports three identities, and they are independent:

| Identity | Where it comes from | How to read it |
|---|---|---|
| Application build | the image, stamped at build time | `fn_build_info` metric, `/healthz`, `fnctl status` |
| Source digest | a hash of the source tree the image was compiled from | same |
| Policy identity | the naming policy version plus a fingerprint of every setting that can change a produced name | same, and stamped on every job |

```sh
docker compose -f compose.prod.yml exec watcher /usr/local/bin/watcher version
curl -s localhost:8080/healthz | jq '{revision, sourceDigest, policyIdentity}'
# or over the private gRPC surface
docker run --rm --network "${FN_PROJECT:-filename-normalizer}_fn" \
    -e FN_GRPC_TARGET=renamer-1:9090 "$FN_IMAGE" fnctl status
```

A job accepted under one policy identity is never reinterpreted under another:
a renamer whose identity differs holds the job as `policy_version_mismatch`.
That is what makes a configuration change visible instead of silent.

**The naming policy is a documented CANDIDATE, not accepted production policy.**
`v1-candidate-2026-09-16`. Its undecided choices are listed in
`docs/filename-normalizer-operations.md` §2.2.

## 2. Placement and sizing

| Service | Count | Why |
|---|---|---|
| `watcher` | exactly **one** | It hosts discovery and completion accounting. Two would both discover the same submissions. |
| `renamer` | **two or more** | Independently scalable. Two is the minimum that demonstrates concurrent progress. |

Bounded concurrency per renamer is `FN_RENAMER_CONCURRENCY` (default 2) with
`FN_RENAMER_PREFETCH` (default 2). Both are tuning, not architecture; raise
them against storage throughput, not against CPU.

**PostgreSQL** must be reachable as the primary. A replica is optional
(`FN_DB_REPLICA_REQUIRED=false` by default) and is read-only. **RabbitMQ**
needs a durable exchange, queue and DLX in the configured vhost; the
applications declare them and will refuse a conflicting redeclare rather than
silently adopt different arguments.

## 3. Storage roles, mounts, permissions

| Role | Written by | Read by | Notes |
|---|---|---|---|
| `incoming` | your scanner/users | watcher | Sources are **never deleted** by this system. |
| `queued` | renamer | renamer | Claimed working files, kept out of ordinary discovery. |
| `staging` | renamer | renamer | Incomplete transfer artifacts. **Must not be visible to Paperless.** |
| `consume` | renamer | **Paperless** | Shared. See below. |
| `failed` | renamer | you | Recoverable quarantine. |

The containers run as uid/gid **65532**. Every bound directory must be
writable by that account, and `consume` must be writable by 65532 **and**
readable by whatever uid Paperless runs as.

### The consume directory has two users

Paperless consumes from it; this system publishes into it. Two consequences:

* **Staging never shares a filesystem role with consume.** A partially
  transferred document must not be reachable by the consumer.
* **The publication intermediates are dotfiles** (`.fn-*`). Paperless ignores
  dotfiles, which is what makes them safe. Publication stages a `.fn-…`
  hard link beside the destination, commits the durable receipt, and only then
  renames it to the real name. A document visible to Paperless therefore always
  has a committed receipt behind it.
* **Configure Paperless to ignore `.fn-*`** if your version does not ignore
  dotfiles by default.

## 4. Secrets

Referenced, never embedded. The applications read passwords from files:

```
FN_SECRET_DB_PASSWORD=/path/outside/this/repo/fn_db_password
FN_SECRET_AMQP_PASSWORD=/path/outside/this/repo/fn_amqp_password
```

Keep them outside the repository, mode 0400 to a directory the deploying user
owns. Nothing in this package contains a credential, and the configuration
schema has no field that could hold one.

`FN_DB_SSLMODE` defaults to `require` here. Lower it only for a database on the
same host.

## 5. Access boundary

Health, metrics and gRPC are **unauthenticated**. The manifest publishes them
on `127.0.0.1` only. Do not expose them to a network you do not control; put
them behind your existing metrics scraper's own access controls.

gRPC (status, configuration inspection, validation, preview) stays private.
It is read-only — there is no runtime configuration mutation, by design.

## 6. Procedures

### First start

```sh
# 1. create the database and role (once, by your DBA process)

# 2. validate the configuration before anything consumes it. Validation and
#    the effective-configuration view are one code path, so "it validates" and
#    "this is what it means" cannot disagree. The output carries no credentials.
docker run --rm --env-file ./prod.env \
    -v "$FN_HOST_CONFIG:/etc/fn/normalizer.json:ro" "$FN_IMAGE" \
    watcher check-config

# 3. apply the schema, as one deliberate run. The services in this manifest
#    have FN_DB_APPLY_MIGRATIONS=false, so none of them will migrate on
#    startup; this is the only thing that does.
docker run --rm --env-file ./prod.env \
    -e FN_DB_APPLY_MIGRATIONS=true -e FN_STORAGE_REQUIRED=false \
    "$FN_IMAGE" watcher check-config

# 4. start
docker compose -f compose.prod.yml --env-file ./prod.env up -d
```

> `check-config` is used for step 3 because migrations are applied by the
> watcher's startup path when `FN_DB_APPLY_MIGRATIONS=true`. There is no
> separate `migrate` subcommand; inventing one in a runbook would be worse
> than naming the mechanism that exists.

### Stop intake (without stopping delivery)

Stop the watcher only. Discovery stops; renamers finish what is already
dispatched, and the broker retains anything queued.

```sh
docker compose -f compose.prod.yml stop watcher
```

Resume by starting it again. Nothing is lost: submissions stay in `incoming`.

### Upgrade

Rolling, one renamer at a time; the watcher last.

```sh
# 1. apply any new migrations FIRST, explicitly, with the new image
docker run --rm --env-file ./prod.env \
    -e FN_DB_APPLY_MIGRATIONS=true -e FN_STORAGE_REQUIRED=false \
    "$FN_NEW_IMAGE" watcher check-config
# 2. replace renamers one at a time, waiting for health between them
FN_IMAGE=$FN_NEW_IMAGE docker compose -f compose.prod.yml up -d --no-deps renamer-1
# ... wait for healthy, then renamer-2
# 3. replace the watcher
FN_IMAGE=$FN_NEW_IMAGE docker compose -f compose.prod.yml up -d --no-deps watcher
```

A renamer whose policy identity differs from a job's holds that job as
`policy_version_mismatch` rather than renaming it differently. During a rolling
upgrade that changes naming, expect held jobs until every instance matches.

### Rollback

**A schema is not rolled back by starting an old image.** The migrations are
forward-only. Rolling back the application while the schema is ahead is
supported only when the new migration was additive; if it was not, the
rollback is a restore (below), not an image change.

```sh
# 1. stop intake
docker compose -f compose.prod.yml stop watcher
# 2. let renamers drain (watch fn_deliveries_in_flight reach 0)
# 3. put the previous image back, renamers first, then the watcher
FN_IMAGE=$FN_PREVIOUS_IMAGE docker compose -f compose.prod.yml up -d
```

Files and durable state are preserved throughout: no procedure here deletes a
source, a receipt, a reservation or an uncertain job.

### Backup and restore

Back up **together**, at the same moment:

1. the PostgreSQL database (`pg_dump -Fc`), and
2. the `consume`, `incoming`, `failed` and `staging` directories.

They are one system. A database restored without its filesystem describes
documents that are not there; a filesystem restored without its database has
documents nothing can explain.

Restore: stop the applications, restore the database, restore the directories,
then start the renamers before the watcher so queued work drains before new
work is discovered.

### Uncertain jobs

An uncertain job is terminal **without intervention, deliberately**. The
document may already be with the consumer, so the system will neither
redeliver it nor call it failed.

```sh
# the processing state, including the uncertain count
docker run --rm --network "${FN_PROJECT:-filename-normalizer}_fn" \
    -e FN_GRPC_TARGET=renamer-1:9090 "$FN_IMAGE" fnctl state

# one job in full, by id
docker run --rm --network "${FN_PROJECT:-filename-normalizer}_fn" \
    -e FN_GRPC_TARGET=renamer-1:9090 "$FN_IMAGE" fnctl inspect <job-id>
```

The job ids themselves come from the ledger; `fnctl` reads, it does not list
by state. Query the database for the ids, then inspect each one.

Resolve one at a time, by hand, using the preserved source. **Never bulk
resolve.** See `docs/filename-normalizer-operations.md`, "Uncertain outcomes".

## 7. Local validation

```sh
make verify-deployment-package
```

It checks this package against the real local stack: the manifest parses with a
complete environment, the image reference is digest-pinned, no secret values
are present, no fault points are set, no application port is published beyond
loopback, every alert expression names a metric the applications actually
export, and the configuration file validates.

## 8. Target qualification — NOT DONE

Everything above is established against local volumes. **Local volumes and
tmpfs do not establish NAS or SMB semantics**, and the contract requires
evidence on the actual mounts:

- atomic publication and no-overwrite on the real consume filesystem
- cross-filesystem delivery between the real incoming and consume
- behaviour when the share disconnects mid-publication
- permission behaviour as uid 65532 on the real export
- a real Paperless instance ingesting from the real consume directory

This requires a concrete target and an explicit authorization. Neither has been
supplied. See the decision packet in the final report.

## 9. What is still unresolved

| Item | Status |
|---|---|
| Alert receivers | **UNRESOLVED** — no notification target supplied |
| Storage capacity alerting | **UNRESOLVED** — the application exports no free-space metric; must come from node-level monitoring |
| Production naming acceptance | **OWNER DECISION** — the policy is a documented candidate |
| Retention and cleanup policy | **OWNER DECISION** — deletion stays disabled and a flag requesting it is refused |
| NAS/SMB qualification | **NOT DONE** — see §8 |
| Automatic failover | **out of scope** — failover is manual and documented |
