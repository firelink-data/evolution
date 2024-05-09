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
* File created: 2023-11-21
* Last updated: 2023-11-21
*/

use std::fs;
use std::fs::File;
use std::path::PathBuf;

use crate::converters::arrow2_converter::{MasterBuilder, Slice2Arrow2};
use crate::converters::arrow_converter::{MasterBuilders, Slice2Arrow};
use crate::converters::self_converter::SampleSliceAggregator;
use crate::converters::Converter;
use crate::dump::dump;
use crate::slicers::chunked_slicer::{OldSlicer, IN_MAX_CHUNKS, SLICER_IN_CHUNK_SIZE};
use crate::slicers::Slicer;
use crate::slicers::{find_last_nl, line_break_len_cr, ChunkAndResidue};
use crate::{converter, error, mocker, schema};
use clap::{value_parser, ArgAction, Parser, Subcommand};
use log::info;
use parquet::arrow::ArrowWriter;
use crate::mocker::Mocker;
use crate::error::Result;
use crate::threads::get_available_threads;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    /// Threads
    #[arg(short, long, action = clap::ArgAction::Count,default_value = "12" )]
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
    Chunks,
    Lines,
}

#[derive(Subcommand)]
enum Commands {
    /// does testing things
    Mock {
        /// Sets schema file
        #[arg(short = 's', long="schema", value_name = "SCHEMA",required = true)]
        schema: PathBuf,
        /// Sets input file
        #[arg(short='o', long="output-file", value_name = "OUTPUT-FILE",required = false)]
        output_file: Option<PathBuf>,
        #[arg(short='n', long="n-rows", value_name = "NUM-ROWS", default_value = "100",required = false)]
        n_rows: Option<usize>,

        /// Set the size of the buffer (number of rows).
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
    CConvert {
        /// Sets schema file

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
    Convert {
        /// The fixed-length file to convert.
        #[arg(
            short = 'f',
            long = "file",
            value_name = "FILE",
            action = ArgAction::Set,
        )]
        file: PathBuf,

        /// Specify output (target) file name.
        #[arg(
            short = 'o',
            long = "output-file",
            value_name = "OUTPUT-FILE",
            action = ArgAction::Set,
            required = false,
        )]
        output_file: PathBuf,

        /// Specify the .json schema file to use when converting.
        #[arg(
            short = 's',
            long = "schema",
            value_name = "SCHEMA",
            action = ArgAction::Set,
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

    Dump {
        /// Sets schema file
        #[clap(value_enum, value_name = "CONVERTER")]
        converter: Converters,
        /// Sets input file
        #[arg(short, long, value_name = "IN-FILE")]
        in_file: PathBuf,
    },
}

impl Cli {
    pub fn run<'a>(
        &self,
    ) -> Result<()> {
        let n_logical_threads = num_cpus::get();
        let mut n_threads: usize = self.n_threads as usize;


        if n_threads > n_logical_threads {
            info!(
                "you specified to use {} thread, but your CPU only has {} logical threads",
                n_threads, n_logical_threads,
            );
            n_threads = n_logical_threads;
        }

        // Effektiv med fixa buffrar men fult att allokeringen ligger här ...känns banalt.

        let multithreaded: bool = n_threads > 1;
        if multithreaded {
            info!("multithreading enabled ({} logical threads)", n_threads);
        }

        match &self.command {
            Some(Commands::Mock {
                schema,
                output_file,
                n_rows,
                buffer_size,
                thread_channel_capacity,
                 }) => {
                print!("target file {:?}", output_file.as_ref());

                Mocker::builder()
                    .schema(schema.to_owned())
                    .output_file(output_file.to_owned())
                    .num_rows(*n_rows)
                    .num_threads(n_threads)
                    .buffer_size(*buffer_size)
                    .thread_channel_capacity(*thread_channel_capacity)
                    .build()?
                    .generate();

                Ok(())
            }

            Some(Commands::Dump {
                converter: _,
                in_file,
            }) => {
                let _in_file = fs::File::open(&in_file).expect("bbb");
                dump(_in_file);

                Ok(())
            }
            Some (Commands::Convert {
                file,
                output_file,
                schema,
                buffer_size,
                thread_channel_capacity,
            }) => {
                converter::Converter::builder()
                    .target_file(file.to_owned())
                    .output_file(output_file.to_owned())
                    .schema(schema.to_owned())
                    .num_threads(n_threads)
                    .buffer_size(*buffer_size)
                    .thread_channel_capacity(*thread_channel_capacity)
                    .build()?
                    .convert();
                  Ok(())
            }



            Some(Commands::CConvert {
                converter,
                slicer: _,
                schema,
                in_file,
                out_file,
            }) => {


                let _in_file = fs::File::open(&in_file).expect("bbb");

                let mut slicer_instance: Box<dyn Slicer> = Box::new(OldSlicer {
                    fn_find_last_nl: find_last_nl,
                });

                let converter_instance: Box<dyn Converter> = match converter {
                    Converters::Arrow => {
                        let mut master_builders = MasterBuilders::builders_factory(
                            schema.to_path_buf(),
                            n_threads as i16,
                        );
                        let writer: ArrowWriter<File> = master_builders.writer_factory(out_file);

                        let s2a: Box<Slice2Arrow> = Box::new(Slice2Arrow {
                            writer: writer,
                            fn_line_break: find_last_nl,
                            fn_line_break_len: line_break_len_cr,
                            masterbuilders: master_builders,
                        });
                        //                        writer.finish().expect("finish");
                        s2a
                    }
                    Converters::Arrow2 => {
                        let _out_file = fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(out_file)
                            .expect("aaa");

                        let master_builder = MasterBuilder::builder_factory(schema.to_path_buf());
                        let s2a: Box<Slice2Arrow2> = Box::new(Slice2Arrow2 {
                            file_out: _out_file,
                            fn_line_break: find_last_nl,
                            master_builder,
                        });

                        s2a
                    }

                    Converters::None => {
                        let _out_file = fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(out_file)
                            .expect("aaa");

                        let s3a: Box<SampleSliceAggregator> = Box::new(SampleSliceAggregator {
                            file_out: _out_file,
                            fn_line_break: find_last_nl,
                        });
                        s3a
                    }
                };

                let r = slicer_instance.slice_and_convert(
                    converter_instance,
                    _in_file,
                    n_threads as usize,
                );
                match r {
                    Ok(s) => {
                        print!(
                            "Operation successful inbytes={} out bytes={} num of rows={}",
                            s.bytes_in, s.bytes_out, s.num_rows
                        );
                    }
                    Err(x) => {
                        print!("Operation failed: {}", x);
                    }
                }

                Ok(())
            }

            //            Ok(()) => todo!(),
            _ => Ok(()),
        }
    }
}
