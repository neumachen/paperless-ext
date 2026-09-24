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
docker compose -f compose.prod.yml exec watcher /usr/local/bin/fn-watcher version
curl -s localhost:8080/healthz | jq '{revision, sourceDigest, policyIdentity}'
# or over the private gRPC surface
docker run --rm --network "${FN_PROJECT:-filename-normalizer}_fn" \
    -e FN_GRPC_TARGET=renamer-1:9090 "$FN_FNCTL_IMAGE" status
```

A job accepted under one policy identity is never reinterpreted under another:
a renamer whose identity differs holds the job as `policy_version_mismatch`.
That is what makes a configuration change visible instead of silent.

**The naming policy is a documented CANDIDATE, not accepted production policy.**
`v1-candidate-2026-09-16`. Review the configuration and preview filenames
before accepting the policy for a production deployment.

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
| `incoming` | your scanner/users; the watcher when archiving | watcher, renamers | Sources are left alone unless `FN_ARCHIVE_ENABLED`: then the watcher moves — or with `FN_ARCHIVE_ACTION=remove`, removes — each delivered original once it is verified (see below). |
| `queued` | renamer | renamer | Claimed working files, kept out of ordinary discovery. |
| `staging` | renamer | renamer | Incomplete transfer artifacts. **Must not be visible to Paperless.** |
| `consume` | renamer | **Paperless** | Shared. See below. |
| `failed` | renamer | you | Recoverable quarantine. |

The containers run as uid/gid **65532**. Every bound directory must be
writable by that account.

`consume` is shared with Paperless and needs more than read access on both
sides: Paperless **removes** the file once it has ingested it, so it needs
write and execute on the directory, not merely read on the file. Where the two
run as different uids, give the directory a group both belong to, mode 2775
(setgid so new files inherit the group). A consumer that cannot remove what it
ingested will re-ingest it.

### Archiving delivered originals out of `incoming`

Off by default. With `FN_ARCHIVE_ENABLED=true` the watcher moves each
delivered original into `FN_ARCHIVE_DIRECTORY` (default `processed`), a
directory **inside** the incoming root — or, with `FN_ARCHIVE_ACTION=remove`,
removes it — so the drop folder holds only what has not been handled yet.
What it will and will not do:

* For a move, the archive directory must exist; it is never created. Its
  absence shows as `fn_source_archive_directory_available 0` and a growing
  `fn_source_archive_waiting`.
* A removal happens only once the original is still the registered file and
  still hashes to what was delivered, and it goes through a private name owned
  by the job, so a document dropped under the same name meanwhile is left
  alone. On a NAS with a recycle bin, removed originals land there.
* The watcher, and only the watcher, then needs **write** on `incoming`. Mount
  it read-write for the watcher and keep it read-only for the renamers.
* Nothing is replaced. The move is a no-replace rename; a name already taken
  in the archive gets the job id before the extension.
* An original that changed after it was delivered stays in the drop folder,
  and so do the originals of held and uncertain jobs.
* Recursive discovery is refused with archiving on: it would descend into the
  archive directory and register every archived original again.

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

Keep them outside the repository. **Mode 0400 owned by the deploying user does
not work**: the containers run as uid 65532 and a bind-mounted file keeps its
host ownership and mode, so 0400 owned by anyone else is unreadable to them.
Either make the files readable by 65532 (`chown 65532` , mode 0400) or use
mode 0440 with a group 65532 belongs to. Verify by starting one service and
confirming it reaches the database rather than assuming.

Nothing in this package contains a credential, and the configuration schema
has no field that could hold one.

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
    -e FN_DB_PASSWORD_FILE=/run/secrets/db -e FN_AMQP_PASSWORD_FILE=/run/secrets/amqp \
    -v "$FN_SECRET_DB_PASSWORD:/run/secrets/db:ro" \
    -v "$FN_SECRET_AMQP_PASSWORD:/run/secrets/amqp:ro" \
    -v "$FN_HOST_CONFIG:/etc/fn/normalizer.json:ro" \
    "$FN_WATCHER_IMAGE" check-config

# 3. apply the schema. Migrations are applied by the WATCHER'S STARTUP PATH
#    when FN_DB_APPLY_MIGRATIONS is true; there is no subcommand that applies
#    them and `check-config` does not -- it loads the configuration, prints
#    the effective view, and returns without touching the database.
#
#    So the first start IS the migration: bring the watcher up with the flag
#    on, wait for it to report ready, then stop it.
FN_DB_APPLY_MIGRATIONS=true \
  docker compose -f compose.prod.yml --env-file ./prod.env up -d watcher
docker compose -f compose.prod.yml --env-file ./prod.env logs -f watcher   # wait for "schema ready"
docker compose -f compose.prod.yml --env-file ./prod.env stop watcher

# 4. confirm the schema the applications expect is the schema installed
curl -s localhost:8080/healthz | jq '{schemaInstalled, schemaExpected}'

# 5. start everything, with migrations off (the manifest's default)
docker compose -f compose.prod.yml --env-file ./prod.env up -d
```

> Three image references, because there are three images: `FN_WATCHER_IMAGE`,
> `FN_RENAMER_IMAGE` and `FN_FNCTL_IMAGE`. Each carries its own entrypoint
> binary, so a subcommand is passed directly rather than selecting a program.
>
> `check-config` is used for step 3 because migrations are applied by the
> watcher's startup path when `FN_DB_APPLY_MIGRATIONS=true`. There is no
> separate `migrate` subcommand; inventing one in a runbook would be worse
> than naming the mechanism that exists.

### Stop intake (without stopping delivery)

Stop the watcher only. Discovery stops; renamers finish what is already
dispatched, and the broker retains anything queued.

```sh
docker compose -f compose.prod.yml --env-file ./prod.env stop watcher
```

Resume by starting it again. Nothing is lost: submissions stay in `incoming`.

### Upgrade

Rolling, one renamer at a time; the watcher last.

```sh
# 1. apply any new migrations FIRST: stop the watcher, bring it back with the
#    new image and the flag on, wait for ready, then continue.
docker compose -f compose.prod.yml --env-file ./prod.env stop watcher
FN_WATCHER_IMAGE=$FN_NEW_WATCHER_IMAGE FN_DB_APPLY_MIGRATIONS=true \
  docker compose -f compose.prod.yml --env-file ./prod.env up -d watcher
# 2. replace renamers one at a time, waiting for health between them
FN_RENAMER_IMAGE=$FN_NEW_RENAMER_IMAGE \
  docker compose -f compose.prod.yml --env-file ./prod.env up -d --no-deps renamer-1
# ... wait for healthy, then renamer-2
# 3. replace the watcher
# The WATCHER image variable, not the renamer's.
FN_WATCHER_IMAGE=$FN_NEW_WATCHER_IMAGE \
  docker compose -f compose.prod.yml --env-file ./prod.env up -d --no-deps watcher
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
docker compose -f compose.prod.yml --env-file ./prod.env stop watcher
# 2. let renamers drain (watch fn_deliveries_in_flight reach 0)
# 3. put the previous image back, renamers first, then the watcher
FN_WATCHER_IMAGE=$FN_PREV_WATCHER_IMAGE FN_RENAMER_IMAGE=$FN_PREV_RENAMER_IMAGE \
  docker compose -f compose.prod.yml --env-file ./prod.env up -d
```

Files and durable state are preserved throughout: no procedure here deletes a
source, a receipt, a reservation or an uncertain job.

### Backup and restore

Back up **together**, as close to one moment as you can manage:

1. the PostgreSQL database (`pg_dump -Fc`), and
2. the `consume`, `incoming`, `failed` and `staging` directories.

They cannot be captured atomically with respect to each other on a running
system. Stop intake first (above) and let deliveries drain, so the window in
which they can disagree is one in which nothing is being published.

They are one system. A database restored without its filesystem describes
documents that are not there; a filesystem restored without its database has
documents nothing can explain.

### Restoring — do NOT restore `incoming` into the live root

Discovery recognises an already-registered submission by **root, name, device
and inode**. A restored file is a new file: it gets a new inode, so it does not
match, and discovery registers it as a NEW submission. Its reserved name is
already taken by the original delivery, so it is delivered a second time under
a collision suffix.

Restoring `incoming` wholesale and restarting therefore **re-delivers every
source that was already delivered**. Content is deliberately not used to
recognise it: two distinct submissions may legitimately hold identical bytes,
and the contract forbids deduplicating them.

The safe procedure:

```sh
# 1. stop everything
docker compose -f compose.prod.yml --env-file ./prod.env down

# 2. restore the database, and consume / failed / staging into place
#    (these are the system's own state; no identity is inferred from them)

# 3. restore `incoming` into a QUARANTINE directory, NOT the live root
mkdir -p /srv/fn/restored-incoming && tar -xf incoming.tar -C /srv/fn/restored-incoming

# 4. start the applications. Nothing in the quarantine is discovered.
FN_DB_APPLY_MIGRATIONS=true \
  docker compose -f compose.prod.yml --env-file ./prod.env up -d watcher
docker compose -f compose.prod.yml --env-file ./prod.env up -d

# 5. reconcile by hand: for each file in the quarantine, ask the ledger whether
#    that source was already delivered.
#      SELECT state, reserved_name FROM jobs WHERE source_name = '<name>';
#    Move only the ones that still need processing into the live incoming root.
```

Uncertain jobs are NOT resolved by a restore, and must not be: a restored
uncertain job is still uncertain, and the document it refers to may already be
with the consumer.

### Uncertain jobs

An uncertain job is terminal **without intervention, deliberately**. The
document may already be with the consumer, so the system will neither
redeliver it nor call it failed.

```sh
# the processing state, including the uncertain count
docker run --rm --network "${FN_PROJECT:-filename-normalizer}_fn" \
    -e FN_GRPC_TARGET=renamer-1:9090 "$FN_FNCTL_IMAGE" state

# one job in full, by id
docker run --rm --network "${FN_PROJECT:-filename-normalizer}_fn" \
    -e FN_GRPC_TARGET=renamer-1:9090 "$FN_FNCTL_IMAGE" inspect <job-id>
```

The job ids themselves come from the ledger; `fnctl` reads, it does not list
by state. Query the database for the ids, then inspect each one.

Resolve one at a time, by hand, using the preserved source. **Never bulk
resolve.** Confirm the receipt and consumer state before deciding what to do.

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

A concrete target has since been supplied: the home-server Kubernetes cluster
and its live Paperless installation. The deployment package for it lives in
the `neumachen/home-server` repository, not here:

* `gitops/03-apps/filename-normalizer/` — watcher, two renamers, storage
* `gitops/02-services/filename-normalizer-db/` — the dedicated CNPG cluster
* `gitops/02-services/rabbitmq/` — the broker the cluster did not have
* `docs/runbooks/filename-normalizer-deployment.md` — target procedures

That package is **desired state only**: nothing is applied, synced or
published, the watcher is committed at `replicas: 0` with discovery disabled,
and no image has been built for the target architecture yet. The qualification
listed above is still NOT done, and this compose manifest remains the local
reference deployment.

One item on the list is now answered rather than pending: the target `consume`
is the Longhorn/ext4 PVC `paperless-consume`, **not** an SMB share, so NAS/SMB
semantics are not on the path to this target. The `paperless-media` SMB share
belongs to Paperless and the Normalizer never touches it.

## 9. What is still unresolved

| Item | Status |
|---|---|
| Proof that an intended share is mounted | **UNRESOLVED** — `FN_STORAGE_REQUIRED` checks that each root is present and usable, which a writable LOCAL mountpoint also satisfies. It does not prove the NAS is mounted there rather than the empty directory underneath it. Check the mount separately before starting |
| Alert receivers | **UNRESOLVED** — no notification target supplied |
| Storage capacity alerting | **UNRESOLVED** — the application exports no free-space metric; must come from node-level monitoring |
| Production naming acceptance | **OWNER DECISION** — the policy is a documented candidate |
| Retention and cleanup policy | **OWNER DECISION** — deletion stays disabled and a flag requesting it is refused. Moving or removing verified delivered originals is available (`FN_ARCHIVE_ENABLED`, `FN_ARCHIVE_ACTION`, off by default); bulk deletion is not |
| NAS/SMB qualification | **NOT DONE** — see §8 |
| Automatic failover | **out of scope** — failover is manual and documented |
