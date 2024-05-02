#!/bin/bash

RUST_LOG=DEBUG ./target/release/evolution -N 16 mock --schema resources/schema/test_schema.json --n-rows $1
