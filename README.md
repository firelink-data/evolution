# evolution

> A robust, platform agnostic, and highly efficient framework for converting old fixed-length files to future-proof targets suitable for analytics and data science.

The evolution project was created as a response to the emergin need for a tool which can transform old fixed-length files to data formats which seamlessly integrate with the modern data analytics landscape, whilst being able to do so fully automatically.

We utilize the native speed of Rust together with multithreading and SIMD techniques to efficiently transform your old fixed-length files (of any size!) to a more modern target. The only target currently implemented is **parquet**, but we aim to implement support for **delta**, **iceberg**, **indradb**, and more.

The project is structured as a monorepo which hosts all of the *evolution* framework components, which can be found under [crates/](crates/) as their own modules. A modular monorepo design of the framework allows anyone to implement their own target converters that can seamlessly integrate with core frameworks existing functionality.


## Installation

The easiest way to install an *evolution* binary on your system with support for all implemented output targets is by using the [Cargo](https://crates.io/) package manager (which downloads it from [this link](https://crates.io/crates/evolution)). This binary can be found at [examples/full](examples/full) in this repo.

```
cargo install evolution

(available features)
 - mock
 - nightly
```

Alternatively you can build everything from source by cloing the repo and compiling using Cargo.

```
git clone https://github.com/firelink-data/evolution.git
cd evolution
cargo build --release
```

If you want to integrate any of the evolution crates in your own project that you're building, simply add them as dependencies to your projects Cargo.toml file like you would any other third-party dependecy, like below.

```toml
[dependencies]
evolution-common = "1.2.0"
evolution-schema = "1.2.0"
```


## Schema setup

To be able to work with automatic file conversion you need to have a valid **schema** available which specifies the structure of the source file you want to convert. A valid schema, in this context, is a json file which adhers to [this template](https://github.com/firelink-data/evolution/tree/main/resources/template-schema.json). If you are unsure whether or not your own schema file is valid according to the template, you can use [this](https://www.jsonschemavalidator.net/) validator tool.

An example schema can be found [here](https://github.com/firelink-data/evolution/blob/main/examples/full/res/example_schema.json), and if you are unsure about valid values for datatypes, alignment modes, and padding symbols, please refer to the [template](https://github.com/firelink-data/evolution/blob/main/examples/full/res/template_schema.json) which lists all valid values. For specifics on all the currently supported padding modes, characters, and default values, please see the [padder](https://github.com/firelink-data/padder) crate (which we also maintain).


## Quick start

If you install the program as explained above then by simply running the binary you will see the following usage print:

```
Efficiently evolve your old fixed-length data files into modern file formats. 

Usage: evolution.exe [OPTIONS] <COMMAND>

Commands:
  convert  Convert a fixed-length file to another file format
  mock     Generate mocked fixed-length files
  help     Print this message or the help of the given subcommand(s)

Options:
  -N, --n-threads <N_THREADS>
          Enable multithreading and set the number of threads (logical cores) to use [default: 1]
  -C, --thread-channel-capacity <THREAD_CHANNEL_CAPACITY>
          The maximum capacity of the thread channel (in number of messages) [default: 32]
  -R, --read-buffer-size <READ_BUFFER_SIZE>
          The size of the read buffer used when converting (in bytes) [default: 5368709120]
  -W, --write-buffer-size <WRITE_BUFFER_SIZE>
          The size of the write buffer used when mocking (in rows) [default: 1000000]
  -h, --help
          Print help
  -V, --version
          Print version
```

To specify the log verbosity, set the `RUST_LOG` environment variable to your wanted value, e.g., `INFO` or `DEBUG`.


## Threading

To know how many threads (logical cores) you have available on your system you can either of the following commands depending on your host system:

- Windows:
    - Command: `Get-WmiObject Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors`
    - Use the value found under **NumberOfLogicalProcessors**.
- Unix:
    - Command: `lscpu | grep -E '^Thread|^Core|^Socket|^CPU\('`
    - The number of logical cores is calculed as: **threads per core X cores per socket X sockets**.


## License
All code is copyright of [firelink](https://github.com/firelink-data/) and published under a general MIT license, please see [LICENSE](https://github.com/firelink-data/evolution/blob/main/LICENSE) for specific information.
