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
* Last updated: 2024-02-05
*/

use clap::{ArgAction, Parser, Subcommand, value_parser};
use log::{info, warn};
use std::path::PathBuf;

use crate::{error, mock, schema};

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

    /// Generate mocked fixed-length files (.flf) for testing purposes.
    Mock {

        /// Specify the .json schema file to mock data for.
        #[arg(
            short = 's',
            long = "schema",
            value_name = "SCHEMA",
            action = ArgAction::Set,
        )]
        schema: PathBuf,

        /// Specify target (output) file name.
        #[arg(
            short = 't',
            long = "target-file",
            value_name = "TARGET-FILE",
            action = ArgAction::Set,
        )]
        target_file: Option<PathBuf>,

        /// Set the number of rows to generate.
        #[arg(
            short = 'n',
            long = "n-rows",
            value_name = "NUM-ROWS",
            default_value = "100",
        )]
        n_rows: Option<usize>
    },
}

fn get_available_threads(n_wanted_threads: usize) -> usize {
    let n_logical_threads: usize = num_cpus::get();

    if n_wanted_threads > n_logical_threads {
        warn!(
            "You specified to use {} threads, but your CPU only has {} logical threads.",
            n_wanted_threads, n_logical_threads,
        );
        info!(
            "Will use all available logical threads ({}).", 
            n_logical_threads,
        );
        return n_logical_threads;
    };

    info!(
        "Executing using {} logical threads.",
        n_wanted_threads,
    );

    n_wanted_threads

}

impl Cli {
    pub fn run(&self) -> Result<(), error::ExecutionError> {
        let n_threads: usize = get_available_threads(self.n_threads);

        let multithreaded: bool = n_threads > 1;
        if multithreaded { info!("Multithreading enabled!") };

        match &self.command {
            Commands::Mock { schema, target_file, n_rows } => {
                mock::Mocker::new(
                    schema::FixedSchema::from_path(schema.to_owned()),
                    target_file.to_owned(),
                    n_threads,
                ).generate(n_rows.unwrap());
            },
        }

        Ok(())
    }
}
