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
// Last updated: 2024-05-15
//

use clap::{value_parser, ArgAction, Parser, Subcommand};
#[cfg(all(feature = "rayon", debug_assertions))]
use log::debug;
#[cfg(feature = "rayon")]
use log::info;
#[cfg(feature = "rayon")]
use parquet::arrow::ArrowWriter;

#[cfg(feature = "rayon")]
use std::fs;
#[cfg(feature = "rayon")]
use std::fs::File;
use std::path::PathBuf;

#[cfg(feature = "rayon")]
use crate::chunked::arrow_converter::{MasterBuilders, Slice2Arrow};
#[cfg(feature = "rayon")]
use crate::chunked::converter::SampleSliceAggregator;
#[cfg(feature = "rayon")]
use crate::chunked::slicer::OldSlicer;
#[cfg(feature = "rayon")]
use crate::chunked::{find_last_nl, line_break_len_cr, Converter as ChunkedConverter, Slicer};
use crate::converter::Converter;
use crate::error::Result;
use crate::mocker::Mocker;
use crate::threads::get_available_threads;

#[cfg(feature = "rayon")]
#[derive(clap::ValueEnum, Clone)]
enum Converters {
    Arrow,
    None,
}

#[cfg(feature = "rayon")]
#[derive(clap::ValueEnum, Clone)]
enum Slicers {
    Chunks,
    Lines,
}

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
            short = 'i',
            long = "in-file",
            value_name = "IN-FILE",
            action = ArgAction::Set,
            required = true,
        )]
        in_file: PathBuf,

        /// Specify output (target) file name.
        #[arg(
            short = 'o',
            long = "out-file",
            value_name = "OUT-FILE",
            action = ArgAction::Set,
            required = true,
        )]
        out_file: PathBuf,

        /// Specify the .json schema file to use when converting.
        #[arg(
            short = 's',
            long = "schema",
            value_name = "SCHEMA",
            action = ArgAction::Set,
            required = true,
        )]
        schema: PathBuf,

        /// Set the size of the buffer (in bytes).
        #[arg(
            long = "buffer-size",
            value_name = "BUFFER-SIZE",
            action = ArgAction::Set,
            required = false,
        )]
        buffer_size: Option<usize>,

        /// Set the capacity of the thread channel (number of messages).
        #[arg(
            long = "thread-channel-capacity",
            value_name = "THREAD-CHANNEL-CAPACITY",
            action = ArgAction::Set,
            required = false,
        )]
        thread_channel_capacity: Option<usize>,
    },

    #[cfg(feature = "rayon")]
    CConvert {
        #[clap(value_enum, value_name = "CCONVERTER")]
        converter: Converters,

        #[clap(value_enum, value_name = "SLICER")]
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

        /// Specify output (target) file name.
        #[arg(
            short = 'o',
            long = "out-file",
            value_name = "OUT-FILE",
            action = ArgAction::Set,
            required = false,
        )]
        out_file: Option<PathBuf>,

        /// Set the number of rows to generate.
        #[arg(
            short = 'n',
            long = "n-rows",
            value_name = "NUM-ROWS",
            default_value = "100",
            required = false
        )]
        n_rows: Option<usize>,

        /// Set the writer mode to create a new file or fail if it already exists.
        #[arg(
            long = "force-new",
            value_name = "WRITER-FORCE-NEW",
            action = ArgAction::SetTrue,
            required = false,
        )]
        writer_force_new: bool,

        /// Set the writer option to truncate a previous file if the out file already exists.
        #[arg(
            long = "truncate-existing",
            value_name = "WRITER-TRUNCATE",
            action = ArgAction::SetTrue,
            required = false,
        )]
        writer_truncate_existing: bool,

        /// Set the size of the buffer (number of rows).
        #[arg(
            long = "buffer-size",
            value_name = "MOCKER-BUFFER-SIZE",
            action = ArgAction::Set,
            required = false,
        )]
        mocker_buffer_size: Option<usize>,

        /// Set the capacity of the thread channel (number of messages).
        #[arg(
            long = "thread-channel-capacity",
            value_name = "MOCKER-THREAD-CHANNEL-CAPACITY",
            action = ArgAction::Set,
            required = false,
        )]
        mocker_thread_channel_capacity: Option<usize>,
    },
}

impl Cli {
    pub fn run(&self) -> Result<()> {
        let n_threads: usize = get_available_threads(self.n_threads);

        #[cfg(feature = "rayon")]
        {
            if n_threads > 1 {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(n_threads)
                    .build_global()
                    .expect("Could not create Rayon thread pool!");
                #[cfg(debug_assertions)]
                debug!("Rayon parallelism enabled!");
            };
        }

        match &self.command {
            Commands::Convert {
                in_file,
                out_file,
                schema,
                buffer_size,
                thread_channel_capacity,
            } => {
                Converter::builder()
                    .with_in_file(in_file.to_owned())
                    .with_out_file(out_file.to_owned())
                    .with_schema(schema.to_owned())
                    .with_num_threads(n_threads)
                    .with_buffer_size(*buffer_size)
                    .with_thread_channel_capacity(*thread_channel_capacity)
                    .build()?
                    .convert()?;
            }
            #[cfg(feature = "rayon")]
            Commands::CConvert {
                converter,
                slicer: _,
                schema,
                in_file,
                out_file,
            } => {
                let _in_file = fs::File::open(&in_file)?;

                let mut slicer_instance: Box<dyn Slicer> = Box::new(OldSlicer {
                    fn_find_last_nl: find_last_nl,
                });

                let converter_instance: Box<dyn ChunkedConverter> = match converter {
                    Converters::Arrow => {
                        let mut master_builders = MasterBuilders::builders_factory(
                            schema.to_path_buf(),
                            n_threads as i16,
                        );
                        let writer: ArrowWriter<File> = master_builders.writer_factory(out_file);

                        let s2a: Box<Slice2Arrow> = Box::new(Slice2Arrow {
                            writer,
                            fn_line_break: find_last_nl,
                            fn_line_break_len: line_break_len_cr,
                            masterbuilders: master_builders,
                        });
                        s2a
                    }
                    Converters::None => {
                        let _out_file = fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(out_file)?;

                        let s3a: Box<SampleSliceAggregator> = Box::new(SampleSliceAggregator {
                            file_out: _out_file,
                            fn_line_break: find_last_nl,
                        });
                        s3a
                    }
                };

                let stats = slicer_instance.slice_and_convert(
                    converter_instance,
                    _in_file,
                    n_threads as usize,
                )?;

                info!(
                    "Operation successful inbytes={} out bytes={} num of rows={}",
                    stats.bytes_in, stats.bytes_out, stats.num_rows
                );
            }
            Commands::Mock {
                schema,
                out_file,
                n_rows,
                writer_force_new,
                writer_truncate_existing,
                mocker_buffer_size,
                mocker_thread_channel_capacity,
            } => {
                Mocker::builder()
                    .with_schema(schema.to_owned())
                    .with_out_file(out_file.to_owned())
                    .with_num_rows(*n_rows)
                    .with_create_new(*writer_force_new)
                    .with_create(true)
                    .with_truncate(*writer_truncate_existing)
                    .with_num_threads(n_threads)
                    .with_buffer_size(*mocker_buffer_size)
                    .with_thread_channel_capacity(*mocker_thread_channel_capacity)
                    .build()?
                    .generate()?;
            }
        }

        Ok(())
    }
}
