.DEFAULT_GOAL := debug

all: clean debug

.PHONY: clean
clean:
	cargo clean

.PHONY: debug
debug:
	cargo build

.PHONY: release
release:
	cargo build --release

.PHONY: test
test:
	cargo test

.PHONY: check
check:
	cargo check

.PHONY: format
format:
	cargo fmt --all

.PHONY: lint
lint:
	cargo clippy --all-features

