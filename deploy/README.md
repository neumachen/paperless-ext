# Deployment

One directory per extension, holding the compose stack, dependency
configuration and orchestration scripts that run it locally.

```text
deploy/
└── filename-normalizer/   # File Normalizer: applications, PostgreSQL cluster, RabbitMQ
```

| Stack | What it runs |
|---|---|
| [`filename-normalizer`](filename-normalizer/README.md) | The watcher and two renamer containers, a primary/standby PostgreSQL cluster, a RabbitMQ node, synthetic document fixtures, and the containerized integration suite. |

## Rules for anything added here

- **Keep environment-specific secrets outside the repository.** A stack may
  generate throwaway local credentials, but they must be gitignored, and no
  credential from an existing real system belongs in this repository.
- **Pin container images by immutable version or digest.**
- **Run application containers non-root**, with a read-only root filesystem and
  dropped capabilities where the runtime allows it.
- **Scope every stack to its own compose project**, so tearing one down removes
  only its own containers, network and volumes and leaves unrelated containers,
  volumes and development data untouched.
- **Publish as little as possible to the host.** Databases and brokers should
  not get host ports; application health and metrics endpoints may.
- **State what a local stack does not establish.** A local synthetic stack
  proves nothing about NAS or SMB mounts, real Paperless-ngx ingestion, or any
  production environment, and its documentation must say so.
