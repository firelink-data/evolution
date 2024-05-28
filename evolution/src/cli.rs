//
// MIT License
//
// Copyright (c) 2024 Firelink Data
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// File created: 2024-02-05
// Last updated: 2024-05-27
//

use clap::{value_parser, ArgAction, Parser, Subcommand};
use evolution_common::error::Result;
use evolution_common::thread::get_available_threads;
#[cfg(feature = "mock")]
use evolution_mocker::mocker::FixedLengthFileMocker;
use evolution_target::target::Target;

use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "evolution",
    author,
    version,
    about,
    long_about = None,
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable multithreading and set the number of threads (logical cores) to use.
    #[arg(
        short = 'N',
        long = "n-threads",
        action = ArgAction::Set,
        default_value = "1",
        value_parser = value_parser!(usize),
        required = false,
    )]
    n_threads: usize,

    /// The size of the read buffer used when converting (in bytes).
    #[arg(
        short = 'R',
        long = "read-buffer-size",
        action = ArgAction::Set,
        // This is exactly 5GB = 5 * 1024 * 1024 * 1024 bytes.
        default_value = "5368709120",
        value_parser = value_parser!(usize),
        required = false,
    )]
    read_buffer_size: usize,

    /// The size of the write buffer used when mocking (in rows).
    #[cfg(feature = "mock")]
    #[arg(
        short = 'W',
        long = "write-buffer-size",
        action = ArgAction::Set,
        // Buffering one million rows, when possible, then writing to file.
        default_value = "1000000",
        value_parser = value_parser!(usize),
        required = false,
    )]
    write_buffer_size: usize,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert a fixed-length file to another file format.
    Convert {
        /// The fixed-length file to convert.
        #[arg(
            short = 'i',
            long = "in-file",
            action = ArgAction::Set,
            required = true,
        )]
        in_file: PathBuf,

        /// The json schema for the input file.
        #[arg(
            short = 's',
            long = "schema",
            action = ArgAction::Set,
            required = true,
        )]
        schema: PathBuf,

        /// The target output file.
        #[arg(
            short = 'o',
            long = "out-file",
            action = ArgAction::Set,
            required = true,
        )]
        out_file: PathBuf,

        /// The type of output to target.
        #[arg(
            short = 't',
            long = "target",
            action = ArgAction::Set,
            default_value = "parquet",
            value_parser = value_parser!(Target),
            required = false,
        )]
        target: Target,
    },

    /// Generate mocked fixed-length files.
    #[cfg(feature = "mock")]
    Mock {
        /// The json schema to generate data based on.
        #[arg(
            short = 's',
            long = "schema",
            action = ArgAction::Set,
            required = true,
        )]
        schema: PathBuf,

        /// The target output file.
        #[arg(
            short = 'o',
            long = "out-file",
            action = ArgAction::Set,
            required = true,
        )]
        out_file: PathBuf,

        /// The number of rows to generate.
        #[arg(
            short = 'n',
            long = "n-rows",
            action = ArgAction::Set,
            default_value = "100000",
            value_parser = value_parser!(usize),
            required = false,
        )]
        n_rows: usize,

        /// Writer option to return an error if the file already exists.
        #[arg(
            long = "force-create-new",
            action = ArgAction::SetTrue,
            required = false,
        )]
        force_create_new: bool,

        /// Writer option to truncate the output file if it already exists.
        #[arg(
            long = "truncate-existing",
            action = ArgAction::SetTrue,
            required = false,
        )]
        truncate_existing: bool,
    },
}

impl Cli {
    pub fn run(&self) -> Result<()> {
        let n_threads: usize = get_available_threads(self.n_threads);
        let read_buffer_size: usize = self.read_buffer_size;
        #[cfg(feature = "mock")]
        let write_buffer_size: usize = self.write_buffer_size;

        match &self.command {
            Commands::Convert {
                in_file,
                schema,
                out_file,
                target,
            } => {
                match target {
                    Target::Delta => todo!(),
                    Target::Iceberg => todo!(),
                    Target::Ipc => todo!(),
                    Target::Parquet => todo!(),
                }
            }
            #[cfg(feature = "mock")]
            Commands::Mock {
                schema,
                out_file,
                n_rows,
                force_create_new,
                truncate_existing,
            } => {
                FixedLengthFileMocker::builder()
                    .with_schema(schema.to_path_buf())
                    .with_out_file(out_file.to_path_buf())
                    .with_num_rows(*n_rows)
                    .with_num_threads(n_threads)
                    .with_write_buffer_size(write_buffer_size)
                    .with_force_create_new(*force_create_new)
                    .with_truncate_existing(*truncate_existing)
                    .try_build()?
                    .try_mock()?;
            }
        };

        Ok(())
    }
}
