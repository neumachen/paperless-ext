# The bounded ten-document pilot — procedure

**Not executed.** This is the procedure, prepared in advance so that the only
things outstanding when it runs are the owner's decisions, not engineering.
Four inputs are deliberately left unresolved; they are listed in §1 and every
one of them is the owner's to supply.

**Nothing here authorises a pilot.** It authorises nothing at all: it is a
document. Running it requires the four inputs and an explicit instruction.

---

## 1. Unresolved inputs — the owner's, not the implementer's

| Input | Why it cannot be chosen here |
|---|---|
| **The ten originals** | Real documents. They must be named explicitly, and the pilot uses COPIES; the originals are never read by the system and never moved. |
| **The target** | Host, and the real `incoming` / `queued` / `staging` / `consume` / `failed` paths. Whether `consume` is NAS/SMB changes what must be qualified first. |
| **Credentials mechanism** | Where the two secret files come from. The local stack's are not reusable. |
| **Naming acceptance** | The policy is a documented CANDIDATE. A pilot that publishes under it produces user-visible names nobody has accepted. |

Until all four exist, the pilot does not start. Missing any one of them is a
stop condition, not a thing to work around.

---

## 2. Preconditions, checked before any document moves

```sh
# storage qualification on the REAL mounts, which local volumes cannot establish
make -C deploy/filename-normalizer verify-deployment-package
make -C deploy/filename-normalizer verify-deployment-rehearsal   # against the target's dependencies
```

- The real `consume` filesystem: atomic publication, no-overwrite, and
  cross-filesystem delivery from the real `incoming`.
- Behaviour when the share disconnects mid-publication.
- Permissions as uid 65532, including that Paperless can **remove** what it
  ingests.
- A real Paperless instance ingesting from the real `consume`.
- `fn_jobs{state="uncertain"}` recorded as a baseline **before** intake.

If any precondition fails, stop. Do not proceed with a subset.

---

## 3. The bound

**Ten documents. One batch. No continuous intake.**

The bound is enforced by what is copied in, not by a setting: the watcher
discovers what is in `incoming`, so putting ten files there and nothing else
is the bound. Do not point the system at a directory that receives more.

```sh
# copies, into a staging area OUTSIDE the watched root
mkdir -p ~/pilot/copies && cp <the ten approved originals> ~/pilot/copies/
sha256sum ~/pilot/copies/* > ~/pilot/manifest.sha256   # the accounting baseline
```

Verify the originals are unchanged afterwards against this manifest. The
system never touches them; this proves it.

---

## 4. Per-document accounting

One row per document, filled in as it moves. A document with no row, or a row
that cannot be completed, stops the pilot.

| # | original name | sha256 | job_id | reserved name | state | receipt | in Paperless | OCR/search |
|---|---|---|---|---|---|---|---|---|

```sh
# job and outcome, per document
SELECT job_id, source_name, state, reserved_name, failure_category
  FROM jobs WHERE source_name = '<name>';
SELECT delivered_name, size_bytes, encode(content_fingerprint,'hex'), absent_observed_at
  FROM delivery_receipts WHERE job_id = '<job_id>';
```

**Ingestion is read from Paperless, not from the directory.** A file
disappearing from `consume` is not ingestion evidence — it is the one thing
the contract says it is not. Query Paperless's own library for the document
and record its id.

**OCR and search**, where the document type supports it: confirm the document
has extracted content and is returned by a search for a term that appears in
it. A document that ingests but has no content is a finding, not a pass.

---

## 5. Outcomes that are not delivery

Every one of these is an expected, accountable result. None of them is a
failure of the pilot; failing to *account* for one is.

| Outcome | What to record | What NOT to do |
|---|---|---|
| **held** | `failure_category`, and where the file is | Do not retry blindly; read the category |
| **uncertain** | the job id, and that the source is preserved | **Do not resolve it.** Compare against the §2 baseline: any new uncertain job is a pilot finding |
| **duplicate** | both job ids; distinct submissions stay distinct | Do not deduplicate |
| **rejected** | which rule rejected it | Do not rename by hand to get it through |
| **quarantined** | the path under `failed/` | Do not delete |

---

## 6. Stop conditions

Stop immediately, and do not submit the remaining documents, on any of:

- a document delivered **twice**, under any name;
- a source file modified, moved or removed by the system;
- an uncertain job appearing that the §2 baseline did not have;
- a document visible in `consume` that no receipt describes;
- Paperless ingesting something the accounting does not have a row for;
- any storage root reporting unavailable;
- the tenth document completing — that is the bound, and reaching it is a
  stop, not a licence to continue.

---

## 7. Rollback

The pilot publishes into a real consume directory that a real Paperless
watches, so "rollback" is not symmetrical: **a document Paperless has already
ingested cannot be un-ingested from here.** That is why the bound is ten.

```sh
# 1. stop intake first; let anything in flight finish
docker compose -f compose.prod.yml --env-file ./prod.env stop watcher
# 2. wait for fn_deliveries_in_flight to reach 0
# 3. stop the renamers
docker compose -f compose.prod.yml --env-file ./prod.env down
```

Then, by hand and per document: anything still in `consume` and not yet
ingested may be removed; anything already in Paperless is removed **in
Paperless**, by a person, using the accounting table from §4. The sources are
untouched throughout, so nothing is lost either way.

Do not restore `incoming` from a backup as part of a rollback — see the
runbook's restore section: restored files get new inodes and would be
re-delivered.

---

## 8. What the pilot does not establish

Ten documents establish that the pipeline works for ten documents on that
storage. They do not establish throughput, long-run stability, retention
behaviour (disabled), or acceptance of the naming policy. Those remain open
after a successful pilot.
