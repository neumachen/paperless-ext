# Paperless Extensions

`paperless-ext` is a language-neutral home for small services and integrations that extend a Paperless-ngx deployment without modifying Paperless itself.

## First extension: File Normalizer

The **File Normalizer** prepares documents before Paperless consumes them. The
watcher application registers completed uploads; RabbitMQ distributes jobs to a
configurable pool of renamer instances that preserve Unicode, normalize
filenames, and safely publish complete documents. Incoming and consumption
directories may reside on different filesystems.

Written in Go, backed by a PostgreSQL cluster and a RabbitMQ broker, shipped as
two independently containerized executables.

See the extension documentation for current capabilities and qualification
limits.

- [Extension documentation](extensions/filename-normalizer/README.md) — the applications, their configuration and telemetry
- [Local stack](deploy/filename-normalizer/README.md) — container-only build, run and test commands

## Working agreement

ChatGPT owns requirements, architectural boundaries, acceptance criteria, and
evidence review; it does not edit repository files or run development
operations. The implementation agent owns code, tests, packaging, and
implementation choices within those boundaries, and applies every repository
change. **The user holds final acceptance and production authorization.**

## Repository layout

```text
paperless-ext/
├── deploy/
│   └── filename-normalizer/   # compose stack, cluster config, orchestration scripts
└── extensions/
    └── filename-normalizer/   # File Normalizer source, tests and packaging
```

Each extension may use the language and runtime best suited to its job. Keep extension-specific source, tests, packaging, and documentation together beneath `extensions/<name>/`, and its deployment example beneath `deploy/<name>/`.

## Principles

- Keep Paperless-ngx unmodified and upgradeable.
- Prefer small, independently deployable services.
- Make filesystem operations recoverable and idempotent.
- Treat document contents and filenames as potentially sensitive: keep names,
  paths, contents, fingerprints and credentials out of ordinary logs and out of
  metric labels.
- Pin deployable artifacts by version or digest, run application containers
  non-root, and download no executable dependencies at startup.
- Run all dependency tooling, formatting, linting, builds, tests and
  application execution inside containers.
- Demonstrate behaviour against real dependencies. No mocks, stubs, in-memory
  substitutes or fake services; anything not demonstrated is reported as unrun.
- Report status honestly. Absence of a file is not proof of success, and a
  consumer acknowledgement is not a completion record.
