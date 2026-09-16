# Paperless Extensions

`paperless-ext` is a language-neutral home for small services and integrations that extend a Paperless-ngx deployment without modifying Paperless itself.

## Initial extension

The first planned extension is a filename normalizer that prepares documents before Paperless consumes them. One watcher registers completed uploads; RabbitMQ distributes jobs to a configurable pool of workers that preserve Unicode, normalize filenames, and safely publish complete documents. Incoming and consumption directories may reside on different filesystems.

See [Filename Normalizer Requirements](docs/filename-normalizer-requirements.md).

## Working agreement

ChatGPT owns requirements, architectural boundaries, acceptance criteria, and
evidence review. The implementation agent owns code, tests, packaging, and
implementation choices within those boundaries.

- [Development roles and decisions](docs/development-workflow.md)
- [First implementation handoff](docs/implementation-handoff-001.md)

## Repository layout

```text
paperless-ext/
├── deploy/       # Deployment examples and packaging added by each extension
├── docs/         # Cross-project requirements and design notes
└── extensions/   # Independently buildable extension services
```

Each extension may use the language and runtime best suited to its job. Keep extension-specific source, tests, packaging, and documentation together beneath `extensions/<name>/`.

## Principles

- Keep Paperless-ngx unmodified and upgradeable.
- Prefer small, independently deployable services.
- Make filesystem operations recoverable and idempotent.
- Treat document contents and filenames as potentially sensitive.
- Pin deployable artifacts by version or digest.
