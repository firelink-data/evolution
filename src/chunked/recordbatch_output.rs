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
use tokio::runtime::Runtime;
use std::env::VarError;
use crate::chunked::trimmer::{trimmer_factory, ColumnTrimmer};
use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use ordered_channel::bounded;
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use tokio::fs::File;
use std::path::{Path, PathBuf};
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use super::{
    RecordBatchOutput, trimmer, ColumnBuilder, Converter, FnFindLastLineBreak, FnLineBreakLen,
    Stats,
};
use crate::schema::FixedSchema;
use crate::chunked;
use crate::cli::Targets;
use crate::datatype::DataType;
use crate::schema;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use atomic_counter::{AtomicCounter, ConsistentCounter};
use crossbeam::atomic::AtomicConsume;
use libc::{bsearch, send};
use ordered_channel::Sender;
use ordered_channel::Receiver;

use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::FileMetaData;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
//use std::sync::mpsc::{sync_channel, Receiver, RecvError, SyncSender};
use std::thread;
//use std::thread::JoinHandle;
use tokio::task::JoinHandle;
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
use tokio::runtime::Builder;
use tokio::time::{sleep};

pub(crate) fn output_factory(
    target: Targets,
    fixed_schema: FixedSchema,
    schema: SchemaRef,
    _outfile: PathBuf,
    rt:& tokio::runtime::Runtime
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

    pfo.setup(schema,fixed_schema, _outfile,rt)

}

pub(crate) struct DeltaOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}

impl DeltaOut {
    async fn deltasetup(schema: FixedSchema) -> Result<(), DeltaTableError> {
        let table_uri = std::env::var("TABLE_URI").map_err(|e| DeltaTableError::GenericError {
            source: Box::new(e),
        })?;
        info!("Using the location of: {:?}", table_uri);

        let table_path = deltalake::Path::parse(&table_uri).unwrap();

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
    async fn myDelta(schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf)->(Result<Stats>) {
        Self::deltasetup(fixed_schema).await.unwrap();
        todo!()
        
    }

}


impl RecordBatchOutput for DeltaOut {

    fn setup(&mut self, schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf,rt:&Runtime) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {

        let j:JoinHandle<Result<Stats>>=rt.spawn(Self::myDelta(schema,fixed_schema,outfile));
        
        
        
//        let dout = Self::deltasetup(fixed_schema);
        
        todo!()
    }
}
pub(crate) struct IcebergOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}
impl RecordBatchOutput for IcebergOut {
    fn setup(&mut self, schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf,rt: &Runtime) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        todo!()
    }
}
pub(crate) struct FlightOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}

impl RecordBatchOutput for FlightOut {
    fn setup(&mut self, schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf,rt: &Runtime) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        todo!()
    }
}


pub(crate) struct ParquetFileOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}
impl ParquetFileOut {
    pub(crate) async fn myParquet(mut receiver:  Receiver<RecordBatch>, schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf)->(Result<Stats>) {
        let _out_file = File::options()
            .create(true)
            .append(true)
            .open(outfile).await.unwrap();
        
        let props = WriterProperties::builder().set_compression(SNAPPY).build();
        let mut writer: AsyncArrowWriter<File> =
            AsyncArrowWriter::try_new(_out_file, schema, Some(props.clone())).unwrap();

            'outer: loop {
                let mut message = receiver.recv();

                match message {
                    Ok(rb) => {
                        writer.write(&rb).await.expect("Error Writing batch");
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
            writer.close();
            Ok(Stats {
                bytes_in: 0,
                bytes_out: 0,
                num_rows: 0,
                read_duration: Default::default(),
                parse_duration: Default::default(),
                builder_write_duration: Default::default(),
            })


    }
}
impl RecordBatchOutput for ParquetFileOut {
    fn setup(&mut self, schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf,rt: &Runtime) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        let (sender, mut receiver) = bounded::<RecordBatch>(100);

        self.sender = Some(sender.clone());

        let j:JoinHandle<Result<Stats>>=rt.spawn(Self::myParquet(receiver,schema,fixed_schema,outfile));

        (self.sender.as_mut().cloned().unwrap(), j)

    }
}

pub struct IpcFileOut {
    pub(crate) sender: Option<Sender<RecordBatch>>,
}


impl RecordBatchOutput for IpcFileOut {
    fn setup(&mut self, schema: SchemaRef,fixed_schema: FixedSchema, outfile: PathBuf,rt: &Runtime) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        todo!()
    }
}
