# File Normalizer foundation: container-only development entry points.
#
# This Makefile only orchestrates containers. It never compiles, formats, lints
# or runs anything on the host: every one of those steps happens inside a
# container built from the pinned toolchain image.

SHELL := /bin/sh
.DEFAULT_GOAL := help

HERE       := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXT_DIR    := $(abspath $(HERE)../../extensions/filename-normalizer)
EVIDENCE   := $(EXT_DIR)/.evidence
COMPOSE    := docker compose
PROJECT    := $(shell sed -n 's/^COMPOSE_PROJECT_NAME=//p' $(HERE).env 2>/dev/null)
PROJECT    := $(if $(PROJECT),$(PROJECT),fnfoundation)

# Pinned image used for the few shell utilities the host workflow needs. It is
# already part of the stack, so this adds no new dependency.
UTIL_IMAGE := postgres:17.11-alpine@sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73

# Build identity. Read from git on the host, which is inspection only.
REVISION   := $(shell git -C $(EXT_DIR) rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
DIRTY      := $(shell test -z "$$(git -C $(EXT_DIR) status --porcelain 2>/dev/null)" || echo -dirty)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

export FN_REVISION   := $(REVISION)$(DIRTY)
export FN_BUILD_DATE := $(BUILD_DATE)

.PHONY: help
help: ## Show the available targets
	@printf 'File Normalizer foundation (project: %s)\n\n' '$(PROJECT)'
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# One-time setup
# ---------------------------------------------------------------------------

.PHONY: env
env: ## Create .env with generated throwaway credentials (never overwrites)
	@if [ -f $(HERE).env ]; then \
	  echo ".env already exists; leaving it untouched."; \
	else \
	  cp $(HERE).env.example $(HERE).env; \
	  for key in FN_DB_PASSWORD FN_REPLICATION_PASSWORD FN_AMQP_PASSWORD; do \
	    secret=$$(docker run --rm $(UTIL_IMAGE) sh -c 'head -c 24 /dev/urandom | base64 | tr -d "\n" | tr "+/=" "xyz"'); \
	    sed -i.bak "s|^$$key=.*|$$key=$$secret|" $(HERE).env; \
	  done; \
	  rm -f $(HERE).env.bak; \
	  chmod 0600 $(HERE).env; \
	  echo "Wrote $(HERE).env with generated local-only credentials."; \
	fi
	@$(MAKE) --no-print-directory secrets
	@mkdir -p $(EVIDENCE)

.PHONY: secrets
secrets: ## Write the application secret files from .env (0700 dir, gitignored)
	@mkdir -p $(HERE)secrets && chmod 0700 $(HERE)secrets
	@for pair in fn_db_password:FN_DB_PASSWORD fn_amqp_password:FN_AMQP_PASSWORD; do \
	  file=$${pair%%:*}; key=$${pair##*:}; \
	  value=$$(sed -n "s/^$$key=//p" $(HERE).env); \
	  if [ -z "$$value" ]; then \
	    echo "error: $$key is empty in $(HERE).env" >&2; exit 1; \
	  fi; \
	  printf '%s' "$$value" > $(HERE)secrets/$$file; \
	  chmod 0644 $(HERE)secrets/$$file; \
	done
	@echo "Wrote $(HERE)secrets/ (fn_db_password, fn_amqp_password)."

# ---------------------------------------------------------------------------
# Build and static verification, both inside containers
# ---------------------------------------------------------------------------

# Both targets go through scripts/run-logged.sh, which tees the output to the
# evidence log AND exits with the containerized command's own status. A plain
# `cmd | tee log` would report tee's status, so a failed verification or build
# would exit 0 and the documented `make build && make test` chain would go on
# to test stale images.

.PHONY: verify
verify: ## Run gofmt, go vet and the unit tests inside the toolchain image
	@mkdir -p $(EVIDENCE)
	@cd $(HERE) && ./scripts/run-logged.sh $(EVIDENCE)/f1-verify.log \
	  docker build --progress=plain --target verify \
	    --build-arg VERSION=$${FN_VERSION:-0.1.0-foundation} \
	    --build-arg REVISION=$(FN_REVISION) \
	    --build-arg BUILD_DATE=$(FN_BUILD_DATE) \
	    -t $(PROJECT)/verify:local $(EXT_DIR)

.PHONY: build
build: env ## Build every image in the stack
	@mkdir -p $(EVIDENCE)
	@cd $(HERE) && ./scripts/run-logged.sh $(EVIDENCE)/f1-build.log \
	  $(COMPOSE) --profile test build --progress=plain

# ---------------------------------------------------------------------------
# Stack lifecycle
# ---------------------------------------------------------------------------

.PHONY: up
up: env ## Start the cluster, broker and applications, then wait for readiness
	cd $(HERE) && $(COMPOSE) up -d --wait --wait-timeout 300
	@$(MAKE) --no-print-directory ready

.PHONY: ready
ready: ## Report each application's readiness and FAIL if any is not ready
	@cd $(HERE) && $(COMPOSE) run --rm --no-deps \
	  --entrypoint /usr/local/bin/fn-renamer renamer-1 \
	  probe --require-ready \
	    http://watcher:8080 http://renamer-1:8080 http://renamer-2:8080

.PHONY: status
status: ## Show container state
	cd $(HERE) && $(COMPOSE) --profile test ps -a

.PHONY: logs
logs: ## Follow application logs
	cd $(HERE) && $(COMPOSE) logs -f watcher renamer-1 renamer-2

.PHONY: down
down: ## Stop the stack, keeping volumes and data
	cd $(HERE) && $(COMPOSE) --profile test down --remove-orphans

.PHONY: down-clean
down-clean: ## Stop the stack and delete only this project's volumes
	cd $(HERE) && $(COMPOSE) --profile test down --remove-orphans --volumes
	@echo "Removed containers, network and volumes for project '$(PROJECT)' only."

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run the full phased integration suite (F1-F6) in containers
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/run-integration.sh

.PHONY: test-baseline
test-baseline: ## Run only the steady-state integration phase
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && FN_TEST_PHASE=baseline $(COMPOSE) run --rm integration -test.v

.PHONY: test-failover
test-failover: ## Exercise manual standby promotion, then rebuild the standby
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/run-failover.sh

.PHONY: test-failure-propagation
test-failure-propagation: ## Prove make verify and make build propagate a containerized failure
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-failure-propagation.sh

.PHONY: test-project-isolation
test-project-isolation: ## Prove an overridden-project failover cannot delete this project's retained volume
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-project-isolation.sh

.PHONY: check-config
check-config: ## Validate the configuration and print the effective, credential-free view
	cd $(HERE) && $(COMPOSE) run --rm --no-deps --entrypoint /usr/local/bin/fn-watcher watcher check-config

.PHONY: submit
submit: ## Submit a document: make submit FILE=path  |  make submit NAME=... TEXT=...
	@cd $(HERE) && if [ -n "$(FILE)" ]; then \
	  base=$$(basename "$(FILE)"); \
	  echo "submitting $$base"; \
	  $(COMPOSE) run --rm --no-deps -T --entrypoint sh -v "$(FILE)":/in/src:ro storage-init -c \
	    'cp /in/src "/srv/fn/incoming/.wip-submit" && mv "/srv/fn/incoming/.wip-submit" "/srv/fn/incoming/$$0"' "$$base"; \
	elif [ -n "$(NAME)" ]; then \
	  echo "submitting $(NAME)"; \
	  $(COMPOSE) run --rm --no-deps -T --entrypoint sh storage-init -c \
	    'printf "%s\n" "$$1" > "/srv/fn/incoming/.wip-submit" && mv "/srv/fn/incoming/.wip-submit" "/srv/fn/incoming/$$0"' \
	    "$(NAME)" "$${TEXT:-synthetic document body}"; \
	else \
	  echo "usage: make submit FILE=<path>   or   make submit NAME=<filename> [TEXT=<body>]" >&2; exit 2; \
	fi
	@echo "The document is written to a temporary name and renamed into place, which is"
	@echo "the strict completion contract. Watch it with: make api ARGS=state"

.PHONY: api
api: ## Call the gRPC inspection API: make api ARGS="status" (see fnctl -h)
	@cd $(HERE) && $(COMPOSE) --profile tools run --rm fnctl $(ARGS)

.PHONY: test-normalization
test-normalization: ## Run only the end-to-end document phase
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && FN_TEST_PHASE=normalization \
	  FN_TEST_RUN_ID=$${FN_TEST_RUN_ID:-manual-$$(date -u +%Y%m%dT%H%M%SZ)} \
	  $(COMPOSE) --profile test run --rm integration -test.v -test.timeout 20m

.PHONY: test-config-restart
test-config-restart: ## Prove accepted configuration meaning survives a real restart (FN-N006)
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-config-restart.sh

.PHONY: test-recovery
test-recovery: ## Interruption, stale worker, filesystem boundary and write failures (A6, A7, A10, A11)
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-recovery.sh

.PHONY: test-dry-run
test-dry-run: ## Prove a dry run preserves sources and the operational ledger (A9)
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-dry-run.sh

.PHONY: test-consumer
test-consumer: ## Run a real isolated Paperless instance against the consume directory and prove the handoff
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-consumer.sh

.PHONY: test-refusal-safety
test-refusal-safety: ## Prove a refusal changes nothing and cleanup respects ownership
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-refusal-safety.sh

.PHONY: test-cleanup-ownership
test-cleanup-ownership: ## Prove the isolation wrapper's own cleanup cannot remove pre-existing resources
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-cleanup-ownership.sh

.PHONY: test-harness-ownership
test-harness-ownership: ## Prove the ownership harness itself preserves pre-existing resources
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/verify-harness-ownership.sh

.PHONY: evidence
evidence: ## Collect logs, readiness and metrics snapshots into .evidence/
	@mkdir -p $(EVIDENCE)
	cd $(HERE) && ./scripts/collect-evidence.sh
	@echo "Evidence written to $(EVIDENCE)"

.PHONY: clean-evidence
clean-evidence: ## Delete this project's collected evidence only
	rm -rf $(EVIDENCE)
	@echo "Removed $(EVIDENCE)"
