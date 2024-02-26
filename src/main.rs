/*
* MIT License
*
* Copyright (c) 2023 Firelink Data
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
* File created: 2023-11-21
* Last updated: 2023-12-14
*/

use crate::Slicers::Old;
use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::{info, SetLoggerError};
use crate::slicers::find_last_nl;
use crate::slicers::old_slicer::old_slicer;
use crate::converters::arrow2_converter::{MasterBuilder, Slice2Arrow2};
use crate::converters::arrow_converter::{in_out, Slice2Arrow};
use crate::converters::self_converter::SampleSliceAggregator;
use crate::converters::Converter;
use crate::slicers::Slicer;

mod logging;
mod mock;
mod schema;
mod converters;
mod slicers;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    /// Threads
    #[arg(short, long, action = clap::ArgAction::Count,default_value = "4" )]
    n_threads: u8,
    //    value_parser(value_parser!(usize))
    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}
#[derive(clap::ValueEnum, Clone)]
enum Converters {
    Arrow,
    Arrow2,
    None,
}

#[derive(clap::ValueEnum, Clone)]
enum Slicers {
    Old,
    New,
}


#[derive(Subcommand)]
enum Commands {
    /// does testing things
    Mock {
        /// Sets schema file
        #[arg(short, long, value_name = "SCHEMA")]
        schema: Option<PathBuf>,
        /// Sets input file
        #[arg(short, long, value_name = "FILE")]
        file: Option<PathBuf>,
        #[arg(short, long, value_name = "n-rows", default_value = "100")]
        n_rows: Option<i64>,
    },
    Convert {
        /// Sets schema file

        #[clap(value_enum,value_name = "CONVERTER")]
        converter: Converters,

        #[clap(value_enum,value_name = "SLICER")]
        slicer: Slicers,

        #[arg(short, long, value_name = "SCHEMA")]
        schema: PathBuf,

        /// Sets input file
        #[arg(short, long, value_name = "IN-FILE")]
        in_file: PathBuf,

        /// Sets input file
        #[arg(short, long, value_name = "OUT-FILE")]
        out_file: PathBuf,
    },
}

///
fn main() -> Result<(), SetLoggerError> {
    logging::setup_log()?;
    let cli = Cli::parse();

    let n_logical_threads = num_cpus::get();
    let mut n_threads: usize = cli.n_threads as usize;

    if n_threads > n_logical_threads {
        info!(
            "you specified to use {} thread, but your CPU only has {} logical threads",
            n_threads, n_logical_threads,
        );
        n_threads = n_logical_threads;
    }

    let multithreaded: bool = n_threads > 1;
    if multithreaded {
        info!("multithreading enabled ({} logical threads)", n_threads);
    }

    match &cli.command {
        Some(Commands::Mock {
            schema,
            file,
            n_rows,
        }) => {
            print!("target file {:?}", file.as_ref());

            mock::mock_from_schema(
                schema.as_ref().expect("REASON").to_path_buf(),
                n_rows.unwrap() as usize,
            );
        }

        Some(Commands::Convert {
            converter,
            slicer,
            schema,
            in_file,
            out_file
             }) => {

            let _in_file = fs::File::open(&in_file).expect("bbb");

            let _out_file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(out_file)
                .expect("aaa");

            let mut slicer_instance: Box<dyn Slicer> = Box::new(old_slicer {});

            let converter_instance: Box<dyn Converter> = match converter {
                Converters::Arrow => {
                    let builders=converters::arrow_converter::in_out_instance_factory (schema.into(),n_threads as i16);
                    let in_out_arrow:Vec<Box<in_out>>=Vec::new();
                    let s2a: Box<Slice2Arrow> = Box::new(Slice2Arrow { file_out: _out_file, fn_line_break: find_last_nl, in_out_arrow: in_out_arrow });
                    s2a
                },
                Converters::Arrow2 => {
                    let master_builder = MasterBuilder::builder_factory(schema);
                    let s2a: Box<Slice2Arrow2> = Box::new(Slice2Arrow2 { file_out: _out_file, fn_line_break: find_last_nl, master_builder });
                    s2a
                },

                Converters::None => {
                    let s3a: Box<SampleSliceAggregator> = Box::new(SampleSliceAggregator { file_out: _out_file, fn_line_break: find_last_nl });
                    s3a
                },

            };


            slicer_instance.slice_and_convert(converter_instance, _in_file, n_threads as usize);

        }

        None => {}
        #[allow(unreachable_patterns)]
        _ => {}
    }

    Ok(())
}
