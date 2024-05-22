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
// File created: 2024-05-07
// Last updated: 2024-05-15
//

use crate::chunked::trimmer::{trimmer_factory, ColumnTrimmer};
use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;
use log::{debug, info};
use ordered_channel::bounded;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use super::{
    arrow_file_output, trimmer, ColumnBuilder, Converter, FnFindLastLineBreak, FnLineBreakLen,
    Stats,
};
use crate::chunked;
use crate::datatype::DataType;
use crate::schema;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use atomic_counter::{AtomicCounter, ConsistentCounter};
use crossbeam::atomic::AtomicConsume;
use libc::bsearch;
use ordered_channel::Sender;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::FileMetaData;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::mpsc::{sync_channel, Receiver, RecvError, SyncSender};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use thread::spawn;
use Compression::SNAPPY;
use crate::cli::Targets;

pub(crate) struct parquet_file_out {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}
pub(crate) fn output_factory (target: Targets,schema: SchemaRef,_outfile:PathBuf) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
    match target {
        Targets::Parquet => {
             let mut pfo: Box<dyn arrow_file_output> = Box::new(parquet_file_out { sender: None });
             pfo.setup(schema, _outfile)
        }
        Targets::IPC => {
            let mut pfo: Box<dyn arrow_file_output> = Box::new(ipc_file_out { sender: None });
            pfo.setup(schema, _outfile)
        }
        Targets::None => {todo!()}
    }
}
impl arrow_file_output for parquet_file_out {
    fn setup(
        &mut self,
        schema: SchemaRef,
        outfile: PathBuf,
    ) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        let _out_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(outfile)
            .expect("aaa");

        let props = WriterProperties::builder().set_compression(SNAPPY).build();

        let mut writer: ArrowWriter<File> =
            ArrowWriter::try_new(_out_file, schema, Some(props.clone())).unwrap();

        let (sender, mut receiver) = bounded::<RecordBatch>(100);

        let t: JoinHandle<Result<Stats>> = thread::spawn(move || {
            'outer: loop {
                let mut message = receiver.recv();

                match message {
                    Ok(rb) => {
                        writer.write(&rb).expect("Error Writing batch");
                        if (rb.num_rows() == 0) {
                            break 'outer;
                        }
                    }
                    Err(e) => {
                        info!("got RecvError in channel , break to outer");
                        break 'outer;
                    }
                }
            }
            info!("closing the writer for parquet");
            writer.finish();
            Ok(Stats {
                bytes_in: 0,
                bytes_out: 0,
                num_rows: 0,
                read_duration: Default::default(),
                parse_duration: Default::default(),
                builder_write_duration: Default::default(),
            })
        });
        (sender, t)
    }
}

pub struct ipc_file_out {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}

impl arrow_file_output for ipc_file_out {
    fn setup(
        &mut self,
        schema: SchemaRef,
        outfile: PathBuf,
    ) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        let _out_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(outfile)
            .expect("aaa");

        let p = IpcWriteOptions::try_with_compression(
            Default::default(),
            Some(CompressionType::LZ4_FRAME),
        );

        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            _out_file,
            &schema,
            Default::default(),
        )
        .expect("TODO: panic message");

        let props = WriterProperties::builder().set_compression(SNAPPY).build();

        let (sender, mut receiver) = bounded::<RecordBatch>(1000);

        let t: JoinHandle<Result<Stats>> = thread::spawn(move || {
            'outer: loop {
                let mut message = receiver.recv();

                match message {
                    Ok(rb) => {
                        writer.write(&rb).expect("Error Writing batch");
                        if (rb.num_rows() == 0) {
                            break 'outer;
                        }
                    }
                    Err(e) => {
                        info!("got RecvError in channel , break to outer");
                        break 'outer;
                    }
                }
            }
            info!("closing the writer for parquet");
            writer.finish();
            Ok(Stats {
                bytes_in: 0,
                bytes_out: 0,
                num_rows: 0,
                read_duration: Default::default(),
                parse_duration: Default::default(),
                builder_write_duration: Default::default(),
            })
        });
        (sender, t)
    }
}
