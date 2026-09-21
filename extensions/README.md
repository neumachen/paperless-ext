# Extensions

Each independently buildable extension lives in its own directory:

```text
extensions/
└── filename-normalizer/   # the File Normalizer (Go)
```

An extension owns its source, tests, dependency lockfiles, container packaging,
and local documentation. The repository intentionally does not mandate one
programming language.

Its deployment example lives beside it under `deploy/<name>/`, so the extension
directory stays buildable on its own and the stack that runs it stays separate.

## Conventions that apply to every extension

- **Container-only development.** Dependency resolution, formatting, linting,
  building, testing and running all happen inside containers. No host language
  runtime is required.
- **Pinned artefacts.** Pin the toolchain and every deployable image by version
  and, where available, digest. Download no executable dependencies at
  container start.
- **Non-root runtime.** Application containers run as an unprivileged account
  with a read-only root filesystem and no capabilities.
- **Externally supplied secrets.** Read credentials from a mounted file or the
  environment; never commit them and never log them.
- **Real dependencies in tests.** No mocks, stubs, in-memory substitutes or
  fake services. A suite fails when a required dependency is absent rather than
  skipping or substituting one.

## Current extensions

| Extension | Language | State |
|---|---|---|
| [`filename-normalizer`](filename-normalizer/README.md) — File Normalizer | Go | See the extension README for current capabilities and qualification limits. |
