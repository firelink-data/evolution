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

🦖 *Evolve your fixed-length data files into Apache Arrow tables, fully parallelized!*

</div>

## 🔎 Overview

...


## 📦 Installation

The easiest way to install *evolution* on your system is by using the [Cargo](https://crates.io/) package manager.
```
cargo install evolution
```

Alternatively, you can build from source by cloning this repo and compiling using Cargo.
```
git clone https://github.com/firelink-data/evolution.git
cd evolution
cargo build --release
```

The program uses either of two different types of threading implementations. The default implementation uses the
standard library threads and has so far proven a more reliable version, the alternative is by using [rayon](https://docs.rs/rayon/latest/rayon/)
for parallel iteration. To use **rayon** instead, build or install the program with the `--features rayon`  flag.


## 🚀 Example usage

If you build and/or install the program as explained above then by simply running the binary you will see the following:
```
🦖 Evolve your fixed-length data files into Apache Arrow tables, fully parallelized!

Usage: evolution [OPTIONS] <COMMAND>

Commands:
  convert  Convert a fixed-length file (.flf) to parquet
  mock     Generate mocked fixed-length files (.flf) for testing purposes
  help     Print this message or the help of the given subcommand(s)

Options:
      --n-threads <NUM-THREADS>  Set the number of threads (logical cores) to use when multi-threading [default: 1]
  -h, --help                     Print help
  -V, --version                  Print version
```

The functionality of the program is structured as two main commands: **mock** and **convert**.

### 👨‍🎨 Mocking

```
Generate mocked fixed-length files (.flf) for testing purposes

Usage: evolution mock [OPTIONS] --schema <SCHEMA>

Options:
  -s, --schema <SCHEMA>
          Specify the .json schema file to mock data for
  -o, --output-file <OUTPUT-FILE>
          Specify output (target) file name
  -n, --n-rows <NUM-ROWS>
          Set the number of rows to generate [default: 100]
      --buffer-size <BUFFER-SIZE>
          Set the size of the buffer (number of rows)
      --thread-channel-capacity <THREAD-CHANNEL-CAPACITY>
          Set the capacity of the thread channel (number of messages)
  -h, --help
          Print help
```

For example, if you wanted to mock 1 billion rows of a fixed-length file from a schema located at `./my/path/to/schema.json` with
the output name `mocked-data.flf`, you could run the following command:
```
evolution mock --schema ./my/schema/path/schema.json --output-file mocked-data.flf --n-rows 1000000000
```

### 🏗️👷‍♂️ Converting

```
Convert a fixed-length file (.flf) to parquet

Usage: evolution convert [OPTIONS] --file <FILE> --schema <SCHEMA>

Options:
  -f, --file <FILE>
          The fixed-length file to convert
  -o, --output-file <OUTPUT-FILE>
          Specify output (target) file name
  -s, --schema <SCHEMA>
          Specify the .json schema file to use when converting
      --buffer-size <BUFFER-SIZE>
          Set the size of the buffer (in bytes)
      --thread-channel-capacity <THREAD-CHANNEL-CAPACITY>
          Set the capacity of the thread channel (number of messages)
  -h, --help
          Print help
```

To convert a fixed-length file called `really-big-data.flf`, with associated schema located at `./my/path/to/schema.json`, to a parquet file with name `smaller-data.parquet`, you could run the following command:
```
evolution convert --file really-big-data.flf --output-file smaller-data.parquet --schema ./my/path/to/schema.json
```

### 🧵 Threading

There exists a global setting for the program called `--n-threads` which dictates whether or not the invoked command will be executed
in single- or multithreaded mode. This argument should be a number representing the number of threads (logical cores) that you want
to use. If you try and set a larger number of threads than you system has logical cores, then the program will use **all available
logical cores**. If this argument is omitted, then the program will run in single-threaded mode.

**Note that running multithreaded only really has any clear increase in performance for substantially large workloads.**

### 🧵 Threading
An experimental multithreaded implementation exists , it reads chunks of 2 megabytes and splits them into n anmounts of cores in O(1). 
Run a small conversion test using the "arrow" converter with slicer type "chunked"
```
$ cargo run --package evolution --release --bin evolution -- convertchunked --schema resources/schema/test_schema.json --in-file resources/schema/test_schema_mock.txt --out-file out.parquet arrow chunked
```

## 📋 License
All code is to be held under a general MIT license, please see [LICENSE](https://github.com/firelink-data/evolution/blob/main/LICENSE) for specific information.
