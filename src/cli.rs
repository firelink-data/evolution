use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::{info, SetLoggerError};
use rand::rngs::mock;
use crate::slicers::{ChunkAndReside, find_last_nl};
use crate::slicers::old_slicer::{IN_MAX_CHUNKS, old_slicer, SLICER_IN_CHUNK_SIZE, SLICER_MAX_RESIDUE_SIZE};
use crate::converters::arrow2_converter::{MasterBuilder, Slice2Arrow2};
use crate::converters::arrow_converter::{InOut, Slice2Arrow};
use crate::converters::self_converter::SampleSliceAggregator;
use crate::converters::Converter;
use crate::{converters, error, mocker, schema};
use crate::slicers::Slicer;


#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
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
        schema: PathBuf,
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

impl Cli {

    pub fn run(& self,in_out_buffers: & mut [ChunkAndReside; 3]) -> Result<(), error::ExecutionError> {

        let n_logical_threads = num_cpus::get();
        let mut n_threads: usize = self.n_threads as usize;

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

        match &self.command {
            Some(Commands::Mock {
                     schema,
                     file,
                     n_rows,
                 }) => {
                print!("target file {:?}", file.as_ref());

                mocker::Mocker::new(
                    schema::FixedSchema::from_path(schema.into()),
                    file.clone(),
                    n_threads,
                )
                    .generate(n_rows.unwrap() as usize);

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

                let mut slicer_instance: Box<dyn  Slicer> = Box::new(old_slicer {
                    fn_line_break: find_last_nl});

                let converter_instance: Box<dyn  Converter> = match converter {
                    Converters::Arrow => {
                        let in_out_arrow = converters::arrow_converter::in_out_instance_factory(schema.to_path_buf(), n_threads as i16);
                        let s2a: Box<Slice2Arrow> = Box::new(Slice2Arrow { file_out: _out_file, fn_line_break: find_last_nl, in_out_arrow: in_out_arrow });
                        s2a
                    },
                    Converters::Arrow2 => {
                        let master_builder = MasterBuilder::builder_factory(schema.to_path_buf());
                        let s2a: Box<Slice2Arrow2> = Box::new(Slice2Arrow2 { file_out: _out_file, fn_line_break: find_last_nl, master_builder });
                        s2a
                    },

                    Converters::None => {
                        let s3a: Box<SampleSliceAggregator> = Box::new(SampleSliceAggregator { file_out: _out_file, fn_line_break: find_last_nl });
                        s3a
                    },
                };


                slicer_instance.slice_and_convert(converter_instance,in_out_buffers, _in_file, n_threads as usize);
            }

            None => {}
            #[allow(unreachable_patterns)]
            _ => {}
        }

        Ok(())
    }
}