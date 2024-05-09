#!/bin/bash

RUST_LOG=DEBUG ./target/release/evolution -N 16 convert -f mocked-test-for-convert.flf -s resources/schema/test_schema.json
