# Continuous integration and image releases

Two workflows live here.

| Workflow | File | What it is for |
|---|---|---|
| CI | [`ci.yml`](ci.yml) | Containerized verification and the real integration suite. |
| Release | [`release.yml`](release.yml) | Validate a release commit, then build and publish the two application images. |

Everything the Go toolchain does — formatting, vet, unit tests, compilation,
the integration test binary, the applications themselves and the inspection
client — happens inside containers built from the pinned toolchain image. The
runner contributes git, Docker, Compose, Make and shell orchestration and
nothing else, which is the same rule the local stack follows.

The dependencies are real. A PostgreSQL primary with a streaming standby, a
RabbitMQ broker, the two applications and their storage volumes are started for
every run. Nothing is mocked, stubbed or replaced with a recorded result, and
`continue-on-error` appears nowhere: a failed command or a failed phase fails
the run.

## CI

### Triggers

- Pull requests targeting `main`.
- Pushes to `main`.
- `workflow_dispatch`, from the Actions tab.
- `workflow_call`, so the release workflow can run these same checks at the
  exact commit it is about to publish.

A superseded pull-request run is cancelled. A push to `main` and a release's
checks are not: those runs are the record for a commit that is going somewhere.

### Jobs

**`verify`** runs `make verify`, which builds the Dockerfile's `verify` stage:
`gofmt -l`, `go vet ./...` and `go test ./internal/... ./cmd/...`. A formatting
or unit-test failure ends the run here, before the long job starts.

**`integration`** runs, in order:

1. `make build` — every image in the `test` profile.
2. `docker compose --profile tools build fnctl` — the inspection client.
   `make build` builds the `test` profile, which does not contain `fnctl`, and
   the suite needs it: the drain phase reads each renamer's effective
   configuration back over the gRPC API with this client, and the restoration
   check that follows treats an unreadable answer as *not restored*. A
   developer machine usually has the image already; a fresh runner does not, so
   it is built explicitly rather than left to an implicit build.
3. `make test` — the existing orchestrator, `scripts/run-integration.sh`, which
   starts the stack itself and walks its phases.

Both jobs run on `ubuntu-24.04`, named explicitly rather than `ubuntu-latest`,
because the images this repository builds and the release platform are
`linux/amd64`.

### Isolation

Every run, and every re-run attempt, gets its own identity from
[`scripts/isolate-stack.sh`](../scripts/isolate-stack.sh):

| | |
|---|---|
| Compose project | `fnci-gh-<run id>-<attempt>-<job>` |
| Image namespace | the same string |
| Image tag | `ci-<run id>-<attempt>` |
| Credentials | generated fresh per run, in a container, never printed |
| Published ports | `FN_CI_PORT_BASE`, +1, +2 (default `18080`) |

The script exists because the Makefile and `scripts/lib.sh` resolve the project
name **differently**. `lib.sh` prefers a shell `COMPOSE_PROJECT_NAME` and falls
back to `.env`, the way Compose itself resolves it; the Makefile reads `.env`
only. A run that exported one name and generated another would have the
Makefile tagging images for one project while the orchestration scripts
stopped, started and removed containers in a different one. The script writes
the name into `.env` *and* exports the same value, then asserts the two agree —
along with the credentials being non-empty and the stack resolving — before
anything is built.

It refuses to run as `fnfoundation`, the developer stack's project, and refuses
to overwrite a `.env` that belongs to a different project.

### Cleanup

[`scripts/teardown-stack.sh`](../scripts/teardown-stack.sh) runs on success,
failure and cancellation. It takes the project name from the generated `.env`,
refuses to proceed if that name is `fnfoundation` or disagrees with the exported
one, then removes this project's containers, network and volumes across all
profiles and this run's images by exact reference. It finishes by counting what
is still labelled for that project and fails if anything survived.

There is no `docker system prune`, `docker volume prune` or `docker image
prune` anywhere. Those are scoped to the daemon, not to a run, and on a machine
that also carries a developer stack they would remove that stack's data.

Cleanup on hard termination or runner loss is **not** arranged and has not been
tested. A step that never starts cannot clean up; on a GitHub-hosted runner the
virtual machine is discarded, which covers that case for the wrong reason.

### Evidence

Each job writes a summary containing the tested commit, the runner
architecture, the Docker server platform, each command's outcome, the
integration phase totals, the phase result lines and a link to its artifact.

[`scripts/collect-reports.sh`](../scripts/collect-reports.sh) copies an explicit
allow-list out of `.evidence`:

- `f1-verify.log`, `f1-build.log`
- `run-metadata.txt`, `phase-results.txt`, `phase-*.log`
- `public-commands/*.txt` — the recorded exit statuses of the public entry points
- one stack snapshot: `compose-ps.txt`, `images.txt`, `image-digests.txt`,
  `container-hardening.txt`, `postgres-cluster.txt`, `postgres-standby.txt`,
  `rabbitmq.txt`, `storage-roles.txt`, and the `/healthz`, `/readyz` and
  `/metrics` documents
- `INDEX.txt`, listing exactly what was collected

Deliberately **not** collected: `.env`, `secrets/`, `.evidence/logs/`,
`.evidence/state/`, the raw per-service logs inside a snapshot, and
`.evidence-retained/`. The checkout is never archived.

Every collected file is then scanned for this run's three generated passwords.
A file that contains one is deleted and the whole collection is refused, so the
upload step does not run. Artifacts are named
`ci-verify-<run>-<attempt>` and `ci-integration-<run>-<attempt>` and are kept
for **14 days**.

## Release

### Triggers

- A pushed tag matching `v*`. The tag must be valid semantic versioning with a
  `v` prefix, for example `v0.1.0`, `v1.2.3-rc.1`. A `v`-prefixed tag that is
  not a supported version ends the run.
- `workflow_dispatch` with a `ref` (a tag or a commit SHA) and a `dry_run`
  checkbox that **defaults to true**.

### The gate

Three independent conditions must hold before anything is pushed.

1. **The commit is resolved once.** The `resolve` job turns the tag into a full
   SHA and every later job is given that SHA, never a branch name.
2. **The checks ran against that commit.** The reusable CI workflow is called
   as `./.github/workflows/ci.yml` — the same commit as the release being made,
   so the checks cannot be a different revision than the one reviewed — with
   `ref` set to the resolved SHA. The publishing job then compares the SHA CI
   reports back with the one it resolved and fails if they differ. A green run
   on some other commit of `main` does not qualify.
3. **The release is approved and the commit is reachable from `main`.**
   Publication requires a version tag whose commit is an ancestor of
   `origin/main`, and the publishing job runs in the `release` environment, so
   the repository owner approves it and the registry credential is scoped to
   it.

There is no `pull_request_target` job and no `workflow_run` artifact bridge, so
nothing built from a fork's code is ever handed a credential. The reusable CI
call carries no `secrets: inherit`; the checks receive no registry credential at
all.

### Jobs

- **`resolve`** — validates the version syntax, resolves the commit, computes
  the reachability answer and records why publication will or will not happen.
- **`ci`** — the reusable workflow above, at the resolved commit.
- **`image`** — builds both targets for `linux/amd64`, loads them into the local
  daemon and runs each application's own `version` command. This job has no
  registry access whatsoever. **It is the dry-run path.**
- **`publish`** — only when the gate allows it. It repeats the build and the
  `version` check *before* logging in, then pushes from the same builder cache.

`BUILD_DATE` is stamped from the release commit's own committer timestamp
rather than the wall clock, so both jobs build identical content and the
artifact validated is the artifact published.

### Images

| Target | Repository | Platform |
|---|---|---|
| `watcher` | `ghcr.io/neumachen/paperless-ext/fn-watcher` | `linux/amd64` |
| `renamer` | `ghcr.io/neumachen/paperless-ext/fn-renamer` | `linux/amd64` |

GHCR rather than Docker Hub because it accepts a nested path,
`ghcr.io/owner/repo/image`, which is what the deployment manifests pin. Docker
Hub has only `namespace/repo` and cannot express it. The path is derived from
`github.repository` and lowercased, so nothing has to be configured to keep it
in step with the repository name.

Dockerfile `extensions/filename-normalizer/Dockerfile`, build context
`extensions/filename-normalizer`.

Each image is pushed with two tags and no others:

- the version, without the `v` — `v0.1.0` becomes `0.1.0`
- `sha-<first 12 of the commit>`

**No `latest` tag is ever written**, so a deployment cannot drift onto a
different artifact without a reviewed change.

`fnctl` is built where the validation stack needs it. Publishing a client image
is outside this pass.

### Identity check before publication

[`scripts/check-image-identity.sh`](../scripts/check-image-identity.sh) runs each
built image's real `version` subcommand — the image's entrypoint is the
application, so this is the binary that would run in production — and requires:

- the reported platform is `linux/amd64`
- the reported version and revision are exactly the ones being released
- a source digest is present and is not `unknown`
- both images report the **same** source digest, so neither was served from a
  stale layer

### Digests

The summary of the `publish` job records, per image, whether the push
succeeded and the **registry manifest digest** to pin. The same values go into a
`release-images.json` artifact named `release-images-<version>`, kept for
**90 days**.

The local image id and the registry digest are recorded under separate names
and are never conflated: an image id names a blob in the runner's own daemon
and cannot be pulled by anything else. Attestations are disabled
(`provenance: false`, `sbom: false`), so the published digest is the image
manifest digest directly.

If one push succeeds and the other fails, the summary says so in those words
and states that the registry holds an incomplete release.

## Registry access

Nothing to configure. GHCR authenticates with the workflow's own
`GITHUB_TOKEN`, so there is no registry secret to store, scope or rotate. The
`publish` job declares `packages: write` and no other job in this repository
does, so nothing else here can push a package, and a pull-request run has no
path to one at all.

What does need doing, **once, by hand, after the first publish**: a package
created by a push is **private**, and nothing in the cluster authenticates to
GHCR — there is no `imagePullSecrets` anywhere in it. Each package's settings
page has *Change visibility → Public*; until that is done the cluster cannot
pull the image it is pinned to. Both packages need it, once each.

The `release` environment still gates publication with required reviewers. It
no longer holds a secret; it holds the approval.

## Running a release

**Dry run — builds and validates, publishes nothing:**

Actions → Release → *Run workflow* → `ref` = the tag or commit → leave
*Build and validate only* ticked. Registry login and push are skipped entirely
and the summary says nothing was published.

**Publishing a version:**

1. Merge to `main`.
2. Tag that commit and push the tag:

```bash
git tag -s v0.1.0 -m 'File Normalizer 0.1.0' && git push origin v0.1.0
```

3. The workflow resolves the tag, runs the full checks against it, builds and
   validates both images, then waits for approval on the `release` environment.
4. Approve it. The images are pushed and the summary carries the two digests.

To publish without a tag push, dispatch with `ref` set to the tag and untick
the dry-run box. A dispatch whose `ref` is a bare commit SHA can never publish:
there is no release tag, and the gate says so.

## Deployment

Publishing an image does not deploy anything. Deployment is a separate,
separately reviewed change in the `home-server` repository that pins the exact
image digest recorded above, applied to the cluster by Argo CD. Nothing in this
repository touches a cluster.

## Local rehearsal

The same entry points, on a machine that may already be running the
`fnfoundation` developer stack. Use a **separate checkout** and a port base that
does not collide:

```bash
git clone /path/to/paperless-ext /tmp/fn-rehearsal && cd /tmp/fn-rehearsal
FN_CI_ID=local-$(date -u +%Y%m%dT%H%M%SZ)-$$ FN_CI_VERSION=local-rehearsal \
  FN_CI_PORT_BASE=18190 .github/scripts/isolate-stack.sh
(cd deploy/filename-normalizer && make verify && make build \
  && docker compose --profile tools build fnctl && make test)
FN_CI_REPORTS=/tmp/fn-rehearsal-reports .github/scripts/collect-reports.sh
.github/scripts/teardown-stack.sh
```

The evidence directory lives inside the checkout, so a separate checkout gets a
separate one and the developer stack's evidence is untouched.

## Checking the workflows themselves

```bash
docker run --rm -v "$PWD":/repo -w /repo \
  docker.io/rhysd/actionlint@sha256:887a259a5a534f3c4f36cb02dca341673c6089431057242cdc931e9f133147e9
```

```bash
docker run --rm -v "$PWD":/repo -w /repo \
  docker.io/koalaman/shellcheck@sha256:61862eba1fcf09a484ebcc6feea46f1782532571a34ed51fedf90dd25f925a8d \
  --severity=style .github/scripts/check-image-identity.sh \
  .github/scripts/collect-reports.sh .github/scripts/isolate-stack.sh \
  .github/scripts/teardown-stack.sh
```

`actionlint` also runs `shellcheck` over every `run:` block.

## What has and has not run on GitHub

**Exercised.** CI runs on hosted `ubuntu-24.04` runners and passes: the verify
job, and the full 21-phase integration suite against the real PostgreSQL pair,
the real broker and the real applications. The per-run isolation, the sanitized
report collection and the scoped teardown all work there.

That first hosted run was worth having. It found three defects that a developer
machine structurally cannot show, all from the same root: Docker Desktop's bind
mounts do not enforce host ownership and its filesystem does not recycle inode
numbers, while a Linux host does both. Two were in the harness; one was a real
identity bug in `internal/storage`. Treat "it passes locally" as weak evidence
for this repository.

**Not exercised.** The release workflow has never run, because no tag has been
pushed. That leaves unproven:

- the `workflow_call` link from the release workflow to CI, and the
  tested-commit comparison that reads its output
- the `release` environment approval gate
- GHCR login, push, and therefore every registry digest
- making the resulting packages public, which the cluster needs and which no
  workflow does

To cut the first release: tag a commit on `main` and push the tag. The workflow
resolves it, runs these checks against that exact commit, builds and validates
both images, then waits for approval. Afterwards, set both packages to public
once.
