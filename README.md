<div align="center">
<br/>
<div align="left">
<br/>
</div>

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Crates.io (latest)](https://img.shields.io/crates/v/evolution)](https://crates.io/crates/evolution)
[![codecov](https://codecov.io/gh/firelink-data/evolution/graph/badge.svg?token=B95DUS13B5)](https://codecov.io/gh/firelink-data/evolution)
[![CI](https://github.com/firelink-data/evolution/actions/workflows/ci.yml/badge.svg)](https://github.com/firelink-data/evolution/actions/workflows/ci.yml)
[![Tests](https://github.com/firelink-data/evolution/actions/workflows/tests.yml/badge.svg)](https://github.com/firelink-data/evolution/actions/workflows/tests.yml)

🦖 *Evolve your fixed length data files into Apache Arrow tables, fully parallelized!*


</div>

## 🔎 Overview

...

## 📦 Installation

The easiest way to install *evolution* on your system is by using the [Cargo](https://crates.io/) package manager.
```
$ cargo install evolution
```

Alternatively, you can build from source by cloning this repo and compiling using Cargo.
```
$ git clone https://github.com/firelink-data/evolution.git
$ cd evolution
$ cargo build --release
```

Run a small conversion test using the "arrow" converter with slicer type "old"
```
$ cargo run --package evolution --release --bin evolution -- convert --schema resources/schema/test_schema.json --in-file resources/schema/test_schema_mock.txt --out-file out.parquet arrow old
```

## 📋 License
All code is to be held under a general MIT license, please see [LICENSE](https://github.com/firelink-data/alloy/blob/main/LICENSE) for specific information.
