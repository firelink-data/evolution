/*
* MIT License
*
* Copyright (c) 2024 Firelink Data
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
* File created: 2024-02-05
* Last updated: 2024-05-05
*/

use clap::{value_parser, ArgAction, Parser, Subcommand};
use log::info;
use std::path::PathBuf;

use crate::error::ExecutionError;
use crate::mocker::Mocker;
use crate::schema::FixedSchema;
use crate::threads::get_available_threads;

#[derive(Parser)]
#[command(
    name = "evolution",
    author,
    version,
    about,
    long_about = None,
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Set the number of threads (logical cores) to use when multi-threading.
    #[arg(
        short = 'N',
        long = "n-threads",
        value_name = "NUM-THREADS",
        action = ArgAction::Set,
        default_value = "1",
        value_parser = value_parser!(usize),
    )]
    n_threads: usize,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert a fixed-length file (.flf) to parquet.
    Convert {
        /// The fixed-length file to convert.
        #[arg(
            short = 'f',
            long = "file",
            value_name = "FILE",
            action = ArgAction::Set,
        )]
        file: PathBuf,

        /// Specify the .json schema file to use when converting.
        #[arg(
            short = 's',
            long = "schema",
            value_name = "SCHEMA",
            action = ArgAction::Set,
        )]
        schema: PathBuf,
    },

    /// Generate mocked fixed-length files (.flf) for testing purposes.
    Mock {
        /// Specify the .json schema file to mock data for.
        #[arg(
            short = 's',
            long = "schema",
            value_name = "SCHEMA",
            action = ArgAction::Set,
            required = true,
        )]
        schema: PathBuf,

        /*
        /// Specify how to write mocked data to disk.
        #[arg(
            short = 'w',
            long = "writer",
            value_name = "WRITER",
            action = ArgAction::Set,
            value_parser = PossibleValuesParser::new([
                 "flf",
                 "arrow",
            ]),
            required = true,
        )]
        writer: String,
        */

        /// Specify output (target) file name.
        #[arg(
            short = 'o',
            long = "output-file",
            value_name = "OUTPUT-FILE",
            action = ArgAction::Set,
            required = false,
        )]
        output_file: Option<PathBuf>,

        /// Set the number of rows to generate.
        #[arg(
            short = 'n',
            long = "n-rows",
            value_name = "NUM-ROWS",
            default_value = "100",
            required = false,
        )]
        n_rows: Option<usize>,
    },
}

impl Cli {
    pub fn run(&self) -> Result<(), ExecutionError> {
        let n_threads: usize = get_available_threads(self.n_threads);

        let multithreaded: bool = n_threads > 1;
        if multithreaded {
            rayon::ThreadPoolBuilder::new()
                .num_threads(n_threads)
                .build_global()
                .expect("Could not create Rayon thread pool!");
            info!("Multithreading enabled!");
        };

        match &self.command {
            Commands::Convert {
                file,
                schema,
            } => {
                /*
                converter::Converter::new(
                    file.to_owned(),
                    schema::FixedSchema::from_path(schema.to_owned()),
                    n_threads,
                )
                .convert();
                */

            },
            Commands::Mock {
                schema,
                n_rows,
                output_file,
            } => {

                let schema = FixedSchema::from_path(schema.to_owned());
                let n_rows = n_rows.expect("Could not parse n_rows from CLI.");
                let output_file = output_file.to_owned();

                Mocker::builder()
                    .schema(schema)
                    .num_rows(n_rows)
                    .num_threads(n_threads)
                    .output_file(output_file)
                    .build()?
                    .generate();
            }
        }

        Ok(())
    }
}
