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

**Take your old and highly inefficient fixed-length files and evolve them into something more efficient, like Apache Parquet!**

This repository hosts the **evolution** program which both allows you to convert existing fixed-length files into other data formats,
but also allows you to create large amounts of mocked data blazingly fast. The program supports full parallelism and utilizes SIMD
techniques, when possible, for highly efficient parsing of data. To get started, follow the installation, setup, and example usage
sections below in this README.

Happy hacking! 👋🥳


## 📋 Table of contents
https://github.com/firelink-data/evolution/edit/feat/single-threaded/README.md
* [Installation](https://github.com/firelink-data/evolution#-installation)
* [Schema setup](https://github.com/firelink-data/evolution#-schema-setup)
* [Example usage](https://github.com/firelink-data/evolution#-example-usage)
  * [Converting]()
  * [Mocking]()
  * [Threading]()
* [License](https://github.com/firelink-data/evolution#-license)


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


## 📝 Schema setup

All available commands in *evolution* require an existing valid **schema**. A schema, in this context, is a [json](https://www.json.org/json-en.html)
file specifying the layout of the contents of a fixed-length file. Every schema used has to follow 
[this](https://github.com/firelink-data/evolution/tree/main/resources/template-schema.json) template. If you are unsure whether or not your own schema
file is valid according to the template, you can use [this](https://www.jsonschemavalidator.net/) validator tool.

An example schema can be found [here](https://github.com/firelink-data/evolution/tree/main/resources/example-schema.json), and looks like this:
```
{
    "name": "EvolutionExampleSchema",
    "version": 1337,
    "columns": [
        {
            "name": "id",
            "offset": 0,
            "length": 9,
            "dtype": "i32",
            "alignment": "Right",
            "pad_symbol": "Zero",
            "is_nullable": false
        },
        {
            "name": "name",
            "offset": 9,
            "length": 32,
            "dtype": "utf8",
            "is_nullable": true
        },
        {
            "name": "city",
            "offset": 41,
            "length": 32,
            "dtype": "utf8",
            "alignment": "Right",
            "pad_symbol": "Backslash",
            "is_nullable": false
        },
        {
            "name": "employed",
            "offset": 73,
            "length": 5,
            "dtype": "boolean",
            "alignment": "Center",
            "pad_symbol": "Asterisk",
            "is_nullable": false
        }
    ]
}
```

As specified in the template, all columns have to provide the following fields **(name, offset, length, is_nullable)**, whereas 
**alignment** and **pad_symbol** can be omitted (as they are in this example for the *name* column). If they are not provided, they will assume their default values which are
"**Right**" and "**Whitespace**" respectively. These default values come from the [padder](https://github.com/firelink-data/padder) crate which defines the enums
`Alignment` and `Symbol`, with default implementations as `Alignment::Right` and `Symbol::Whitespace` respectively.


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

As you can see from above, the functionality of the program comprises of the two main commands: **convert** and **mock**.


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


### 🧵 Threading

There exists a global setting for the program called `--n-threads` which dictates whether or not the invoked command will be executed
in single- or multithreaded mode. This argument should be a number representing the number of threads (logical cores) that you want
to use. If you try and set a larger number of threads than you system has logical cores, then the program will use **all available
logical cores**. If this argument is omitted, then the program will run in single-threaded mode.

**Note that running multithreaded only really has any clear increase in performance for substantially large workloads.**

If you are unsure how many logical cores your CPU has, the easiest way to find out is by simply running the program with the
`--n-threads` option set to a large number. The program will check how many logical cores you have and see whether
this option exceeds the possible value. If the value you passed is greater than the number of logical cores on your system, then
the number of logical cores available will be logged to you on stdout.

You could also potentially use one of the commands below depending on your host system.

### Windows
```
Get-WmiObject Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors
```

Use the value found under **NumberOfLogicalProcessors**.

### Unix
```
lscpu | grep -E '^Thread|^Core|^Socket|^CPU\('
```

The number of logical cores is calculed as: **threads per core X cores per socket X sockets**.


## 📜 License
All code is to be held under a general MIT license, please see [LICENSE](https://github.com/firelink-data/evolution/blob/main/LICENSE) for specific information.
