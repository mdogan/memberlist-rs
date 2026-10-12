CARGO ?= cargo

# Extra arguments for the test targets, for example:
#   make test ARGS="state_test"
#   make test-unit ARGS="-- --nocapture"
ARGS ?=

.PHONY: build release test test-unit test-interop fmt lint check clean help

build: ## Build the library (debug)
	$(CARGO) build

release: ## Build the library (release)
	$(CARGO) build --release

test: ## Run all tests (the interop tests are skipped without the Go toolchain)
	$(CARGO) test $(ARGS)

test-unit: ## Run the unit tests only
	$(CARGO) test --lib $(ARGS)

test-interop: ## Run the Go interop tests only (needs the Go toolchain)
	$(CARGO) test --test interop $(ARGS)

fmt: ## Format the code
	$(CARGO) fmt

lint: ## Run clippy, warnings are errors
	$(CARGO) clippy --all-targets -- -D warnings

check: fmt lint test ## Run fmt, lint and test

clean: ## Remove the build output
	$(CARGO) clean

help:
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
