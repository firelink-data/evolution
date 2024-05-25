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
// Last updated: 2024-05-25
//

use std::env::VarError;
use crate::chunked::trimmer::{trimmer_factory, ColumnTrimmer};
use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use ordered_channel::bounded;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use super::{
    RecordBatchOutput, trimmer, ColumnBuilder, Converter, FnFindLastLineBreak, FnLineBreakLen,
    Stats,
};
use crate::chunked;
use crate::cli::Targets;
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
use chrono::prelude::*;
use deltalake::arrow::array::*;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::{ PrimitiveType, StructField, StructType};
use deltalake::parquet::{
    basic::{ZstdLevel},
    
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::*;
//use deltalake::writer::test_utils::create_initialized_table;
use tracing::*;


pub(crate) fn output_factory(
    target: Targets,
    schema: SchemaRef,
    _outfile: PathBuf,
) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
    let mut pfo: Box<dyn RecordBatchOutput> =match target {
        Targets::Parquet => {
             Box::new(ParquetFileOut { sender: None })
        }
        Targets::IPC => {
             Box::new(IpcFileOut { sender: None })
        }
        Targets::Iceberg => {
            Box::new(IcebergOut { sender: None })
        }
        Targets::Delta => {
            Box::new(DeltaOut { sender: None })
        }
        Targets::Flight => {
            Box::new(FlightOut { sender: None })
        }
        Targets::Orc => {
            todo!()
        }
        Targets::None => {
            todo!()
        }

    };

    pfo.setup(schema, _outfile)

}

pub(crate) struct DeltaOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}

impl DeltaOut {
    async fn deltasetup(schema: FixedSchema) -> Result<parquet::errors::ParquetError> {
        let table_uri = std::env::var("TABLE_URI").map_err(|e| DeltaTableError::GenericError {
            source: Box::new(e),
        })?;
        info!("Using the location of: {:?}", table_uri);

        let table_path = deltalake::Path::parse(&table_uri)?;

        let maybe_table = deltalake::open_table(&table_path).await;
        let mut table = match maybe_table {
            Ok(table) => table,
            Err(DeltaTableError::NotATable(_)) => {
                info!("It doesn't look like our delta table has been created");
                DeltaOps::try_from_uri(table_path)
                    .await
                    .unwrap()
                    .create()
                    .with_columns(schema.into_delta_columns())
                    .await
                    .unwrap()

            }
            Err(err) => panic!("{:?}", err),
        };

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let mut writer = RecordBatchWriter::for_table(&table)
            .expect("Failed to make RecordBatchWriter")
            .with_writer_properties(writer_properties);

    todo!()
    }
}

impl RecordBatchOutput for DeltaOut {
    
    fn setup(&mut self, schema: FxiedSchema, outfile: PathBuf) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        let dout = DeltaOut.deltasetup()?
        todo!()
    }
}
pub(crate) struct IcebergOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}
impl RecordBatchOutput for IcebergOut {
    fn setup(&mut self, schema: SchemaRef, outfile: PathBuf) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        todo!()
    }
}
pub(crate) struct FlightOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}

impl RecordBatchOutput for FlightOut {
    fn setup(&mut self, schema: SchemaRef, outfile: PathBuf) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        todo!()
    }
}


pub(crate) struct ParquetFileOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}
impl RecordBatchOutput for ParquetFileOut {
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
        self.sender = Some(sender.clone());
        (sender, t)
    }
}

pub struct IpcFileOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}


impl RecordBatchOutput for IpcFileOut {
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
        self.sender = Some(sender.clone());
        (sender, t)
    }
}
