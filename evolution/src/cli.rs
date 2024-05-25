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
// Last updated: 2024-05-25
//

use clap::{value_parser, ArgAction, Parser, Subcommand};
use evolution_common::error::Result;
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
    },
}

impl Cli {
    pub fn run(&self) -> Result<()> {
        match &self.command {
            Commands::Convert {
                in_file,
                schema,
                out_file,
                target,
            } => {
                todo!()
            },
            Commands::Mock {
                schema,
                out_file,
                n_rows,
            } => {
                todo!()
            }
        }
    }
}

