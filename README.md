# Evolution
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Crates.io (latest)](https://img.shields.io/crates/v/evolution)](https://crates.io/crates/evolution)
[![codecov](https://codecov.io/gh/firelink-data/evolution/graph/badge.svg?token=B95DUS13B5)](https://codecov.io/gh/firelink-data/evolution)
[![CI](https://github.com/firelink-data/evolution/actions/workflows/ci.yml/badge.svg)](https://github.com/firelink-data/evolution/actions/workflows/ci.yml)
[![CD](https://github.com/firelink-data/evolution/actions/workflows/cd.yml/badge.svg)](https://github.com/firelink-data/evolution/actions/workflows/cd.yml)
[![Tests](https://github.com/firelink-data/evolution/actions/workflows/tests.yml/badge.svg)](https://github.com/firelink-data/evolution/actions/workflows/tests.yml)

> 🦖 A modern, platform agnostic, and highly efficient framework that makes it easy to convert and evolve<br/>fixed-length files into robust and future-proof targets, such as, but not limited to, **Parquet**, **Delta**, or **Iceberg**.

This repository hosts the **evolution** project which both allows you to convert existing fixed-length files into other data formats, but also allows you to create large amounts of mocked data blazingly fast. The program supports full parallelism and utilizes SIMD techniques, when possible, for highly efficient parsing of data. 

To get started, follow the installation, schema setup, and example usage sections below in this README.


## 📋 Table of contents

All of the code in this repo is open source and should be licensed according to [LICENSE](https://github.com/firelink-data/evolution/blob/main/LICENSE), refer to [this](https://github.com/firelink-data/evolution?tab=readme-ov-file#-license) link for more information.

* [Installation](https://github.com/firelink-data/evolution?tab=readme-ov-file#-installation)
* [Schema setup](https://github.com/firelink-data/evolution?tab=readme-ov-file#-schema-setup)
* [Example usage](https://github.com/firelink-data/evolution?tab=readme-ov-file#-example-usage)
  * [Converting](https://github.com/firelink-data/evolution?tab=readme-ov-file#%EF%B8%8F%EF%B8%8F-converting)
  * [Mocking](https://github.com/firelink-data/evolution?tab=readme-ov-file#-mocking)
  * [Threading](https://github.com/firelink-data/evolution?tab=readme-ov-file#-threading)


## 📦 Installation

The easiest way to install an **evolution** binary on your system is by using the [Cargo](https://crates.io/) package manager (which downloads it from [this](https://crates.io/crates/evolution) link).
```
$ cargo install evolution
    
(available features)    
 - rayon
 - nightly
```

Alternatively, you can build from source by cloning the repo and compiling using Cargo. See below for available optional features.
```
$ git clone https://github.com/firelink-data/evolution.git
$ cd evolution
$ cargo build --release

(optional: copy the binary to your users binray folder)
$ cp ./target/release/evolution /usr/bin/evolution
```

- Installing with the **rayon** feature will utilize the [rayon](https://docs.rs/rayon/latest/rayon/) crate for parallel execution instead of the standard library threads. It also enables converting in **chunked** mode. Please see [this](https://github.com/firelink-data/evolution?tab=readme-ov-file#%EF%B8%8F%EF%B8%8F-converting) reference for more information.
- Installing with the **nightly** feature will use the [nightly](https://doc.rust-lang.org/book/appendix-07-nightly-rust.html) toolchain, which in nature is unstable. To be able to run this version you need the nightly toolchain installed on your system. You can install this by running `rustup install nightly` from your shell.


## 📝 Schema setup

All available commands in **evolution** require an existing valid **schema**. A schema, in this context, is a [json](https://www.json.org/json-en.html) file specifying the layout of the contents of a fixed-length file (flf). Every schema used has to adhere to [this](https://github.com/firelink-data/evolution/tree/main/resources/template-schema.json) template. If you are unsure whether or not your own schema
file is valid according to the template, you can use [this](https://www.jsonschemavalidator.net/) validator tool.

An example schema can be found [here](https://github.com/firelink-data/evolution/blob/main/resources/example_schema.json), and looks like this:
```
{
    "name": "EvolutionExampleSchema",
    "version": 1337,
    "columns": [
        {
            "name": "id",
            "offset": 0,
            "length": 9,
            "dtype": "Int32",
            "alignment": "Right",
            "pad_symbol": "Underscore",
            "is_nullable": false
        },
        {
            "name": "name",
            "offset": 9,
            "length": 32,
            "dtype": "Utf8",
            "is_nullable": true
        },
        {
            "name": "city",
            "offset": 41,
            "length": 32,
            "dtype": "Utf8",
            "alignment": "Right",
            "pad_symbol": "Backslash",
            "is_nullable": false
        },
        {
            "name": "employed",
            "offset": 73,
            "length": 5,
            "dtype": "Boolean",
            "alignment": "Center",
            "pad_symbol": "Asterisk",
            "is_nullable": true
        }
    ]
}
```

- If you are unsure about valid values for the **dtype**, **alignment**, and **pad_symbol** fields, please referr to the template which lists all valid values.
- All columns have to provide the following fields **name**, **offset**, **length**, and **is_nullable**, whereas  **alignment** and **pad_symbol** can be omitted (as they are in this example for the *name* column). If they are not provided, they will assume their default values which are "**Right**" and "**Whitespace**". 
- The default values come from the [padder](https://github.com/firelink-data/padder) crate which defines the enums
`Alignment` and `Symbol`, with default implementations as `Alignment::Right` and `Symbol::Whitespace` respectively.


## ⚡️ Quick start

If you install the program as explained above then by simply running the binary you will see the following helpful usage print:
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

As you can see from above, the functionality of the program comprises of the two main commands: **convert** and **mock**. If you installed the program with the **rayon** feature you will also have access to a third command called **c-convert**. This stands for **chunked-convert** and is an alternative implementation. Documentation for this command is work-in-progress.

- If you want to see debug prints during execution, set the `RUST_LOG` environment variable to `DEBUG` before executing the program.


### 🏗️👷‍♂️ Converting

```
Convert a fixed-length file (.flf) to parquet

Usage: evolution convert [OPTIONS] --in-file <IN-FILE> --out-file <OUT-FILE> --schema <SCHEMA>

Options:
  -i, --in-file <IN-FILE>
          The fixed-length file to convert
  -o, --out-file <OUT-FILE>
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

To convert a fixed-length file called `old-data.flf`, with associated schema located at `./my/path/to/schema.json`, to a parquet file with name `converted.parquet`, you could run the following command:
```
$ evolution convert --in-file old-data.flf --out-file converted.parquet --schema ./my/path/to/schema.json
```


### 👨‍🎨 Mocking

```
Generate mocked fixed-length files (.flf) for testing purposes

Usage: evolution mock [OPTIONS] --schema <SCHEMA>

Options:
  -s, --schema <SCHEMA>
          Specify the .json schema file to mock data for
  -o, --out-file <OUT-FILE>
          Specify output (target) file name
  -n, --n-rows <NUM-ROWS>
          Set the number of rows to generate [default: 100]
      --force-new
          Set the writer option to fail if the file already exists
      --truncate-existing
          Set the writer option to truncate a previous file if the out file already exists
      --buffer-size <MOCKER-BUFFER-SIZE>
          Set the size of the buffer (number of rows)
      --thread-channel-capacity <MOCKER-THREAD-CHANNEL-CAPACITY>
          Set the capacity of the thread channel (number of messages)
  -h, --help
          Print help
```

For example, if you wanted to mock 1 billion rows of a fixed-length file from a schema located at `./my/path/to/schema.json` with the output name `mocked-data.flf` and enforce that the file should not already exist, you could run the following command:
```
$ evolution mock --schema ./my/path/to/schema.json --out-file mocked-data.flf --n-rows 1000000000 --force-new
```


### 🧵 Threading

There exists a global setting for the program called `--n-threads` which dictates whether or not the invoked command will be executed in single- or multithreaded mode. This argument should be a number representing the number of threads (logical cores) that you want to use. If you try and set a larger number of threads than you system has logical cores, then the program will use **all available logical cores**. If this argument is omitted, then the program will run in single-threaded mode.

**Note that running in multithreaded mode only really has any clear increase in performance for substantially large workloads.**

If you are unsure how many logical cores your CPU has, the easiest way to find out is by simply running the program with the `--n-threads` option set to a large number. The program will check how many logical cores you have and see whether this option exceeds the possible value. If the value you passed is greater than the number of logical cores on your system, then the number of logical cores available will be logged to you on stdout.

You could also potentially use one of the commands below depending on your host system.

### Windows
```
$ Get-WmiObject Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors
```

Use the value found under **NumberOfLogicalProcessors**.

### Unix
```
$ lscpu | grep -E '^Thread|^Core|^Socket|^CPU\('
```

The number of logical cores is calculed as: **threads per core X cores per socket X sockets**.


## ⚠️ License
All code is to be held under a general MIT license, please see [LICENSE](https://github.com/firelink-data/evolution/blob/main/LICENSE) for specific information.
