# Paperless Extensions

`paperless-ext` is a language-neutral home for small services and integrations that extend a Paperless-ngx deployment without modifying Paperless itself.

## Initial extension

The first planned extension is a filename normalizer that prepares documents before Paperless consumes them. It will normalize human-readable filenames, coordinate durable jobs through RabbitMQ, and safely move completed files between directories on an SMB share.

See [Filename Normalizer Requirements](docs/filename-normalizer-requirements.md).

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
