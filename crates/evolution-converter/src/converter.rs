//
// MIT License
//
// Copyright (c) 2023-2024 Firelink Data
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
// File created: 2024-02-17
// Last updated: 2024-10-09
//

use arrow::buffer;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use crossbeam::{channel, scope, thread};
use crossbeam::thread::ScopedJoinHandle;

use evolution_builder::builder::ParquetBuilder;
use evolution_common::error::{Result, SetupError};
use evolution_common::thread::estimate_best_thread_channel_capacity;
use evolution_common::NUM_BYTES_FOR_NEWLINE;
use evolution_schema::schema::FixedSchema;
use evolution_slicer::slicer::{FileSlicer, Slicer};
use evolution_writer::parquet::ParquetWriter;

#[cfg(debug_assertions)]
use log::debug;
use log::{info, Record};
use parquet::file::properties::WriterProperties as ArrowWriterProperties;

use std::path::PathBuf;
use std::sync::Arc;

///
pub trait Converter {}

///
pub type ConverterRef = Box<dyn Converter>;

///
pub struct ParquetConverter {
    slicer: FileSlicer,
    writer: ParquetWriter,
    builder: ParquetBuilder,
    schema: FixedSchema,
    read_buffer_size: usize,

    n_threads: usize,
    thread_channel_capacity: usize,
}

impl ParquetConverter {
    /// Create a new instance of a [`ParquetConverterBuilder`] with default values.
    pub fn builder() -> ParquetConverterBuilder {
        ParquetConverterBuilder {
            ..Default::default()
        }
    }

    ///
    pub fn try_convert(&mut self) -> Result<()> {
        if self.n_threads > 1 {
            self.try_convert_multithreaded()?;
        } else {
            self.try_convert_single_threaded()?;
        }
        Ok(())
    }

    /// Converts the target file in multithreaded mode.
    ///
    /// # Panics ?
    /// ...
    ///
    /// # Errors
    /// ...
    pub fn try_convert_multithreaded(&mut self) -> Result<()> {
        let mut buffer_capacity = self.read_buffer_size;
        let n_worker_threads: usize = self.n_threads - 1;

        info!("Converting flf to parquet in multithreaded mode.");
        info!(
            "The file to convert is {} bytes in total.",
            self.slicer.bytes_to_read(),
        );

        let mut line_break_indices: Vec<usize> =
            Vec::with_capacity(buffer_capacity);

        let mut worker_line_break_indices: Vec<(usize, usize)> =
            Vec::with_capacity(n_worker_threads);

        loop {
            if self.slicer.is_done() {
                break;
            }

            let mut remaining_bytes: usize = self.slicer.remaining_bytes();
            let mut bytes_processed: usize = self.slicer.bytes_processed();
            let mut bytes_overlapped: usize = self.slicer.bytes_overlapped();

            if remaining_bytes < buffer_capacity {
                buffer_capacity = remaining_bytes;
            }

            let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];
            self.slicer.try_read_to_buffer(&mut buffer)?;
            self.slicer.try_find_line_breaks(&buffer, &mut line_break_indices)?;

            let byte_idx_last_line_break: usize = self.slicer.try_find_last_line_break(&buffer)?;
            let n_bytes_left_after_last_line_break: usize =
                buffer_capacity - byte_idx_last_line_break - NUM_BYTES_FOR_NEWLINE;

            self.distribute_worker_thread_workloads(
                &line_break_indices,
                &mut worker_line_break_indices,
            );

            self.spawn_threads(
                &buffer,
                &line_break_indices,
                &worker_line_break_indices,
            )?;

            self.slicer
                .try_seek_relative(-(n_bytes_left_after_last_line_break as i64))?;

            bytes_processed += buffer_capacity - n_bytes_left_after_last_line_break;
            bytes_overlapped += n_bytes_left_after_last_line_break;
            remaining_bytes -= buffer_capacity - n_bytes_left_after_last_line_break;

            self.slicer.set_remaining_bytes(remaining_bytes);
            self.slicer.set_bytes_processed(bytes_processed);
            self.slicer.set_bytes_overlapped(bytes_overlapped);

        }

        Ok(())
    }

    /// Converts the target file in single-threaded mode.
    ///
    /// # Panics ?
    /// ...
    ///
    /// # Errors
    /// ...
    pub fn try_convert_single_threaded(&mut self) -> Result<()> {
        let mut buffer_capacity: usize = self.read_buffer_size;

        info!("Converting flf to parquet in single-threaded mode.");
        info!(
            "The file to convert is {} bytes in total.",
            self.slicer.bytes_to_read(),
        );

        loop {
            if self.slicer.is_done() {
                break;
            }

            let mut remaining_bytes: usize = self.slicer.remaining_bytes();
            let mut bytes_processed: usize = self.slicer.bytes_processed();
            let mut bytes_overlapped: usize = self.slicer.bytes_overlapped();

            if remaining_bytes < buffer_capacity {
                buffer_capacity = remaining_bytes;
            }

            // This is really ugly, I dont want to have to perform these syscalls
            // for heap allocations in the main loop... But the buffered reader
            // requires that the buffer contains some values, it can't be empty
            // but with large enough capacity, really dumb. Look into smarter
            // way to do this, because it really bothers me!!!!!
            let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];
            self.slicer.try_read_to_buffer(&mut buffer)?;

            let byte_idx_last_line_break: usize = self.slicer.try_find_last_line_break(&buffer)?;
            let n_bytes_left_after_last_line_break: usize =
                buffer_capacity - byte_idx_last_line_break - NUM_BYTES_FOR_NEWLINE;

            self.builder.try_build_from_slice(&buffer)?;
            self.writer.try_write_from_builder(&mut self.builder)?;

            self.slicer
                .try_seek_relative(-(n_bytes_left_after_last_line_break as i64))?;

            bytes_processed += buffer_capacity - n_bytes_left_after_last_line_break;
            bytes_overlapped += n_bytes_left_after_last_line_break;
            remaining_bytes -= buffer_capacity - n_bytes_left_after_last_line_break;

            self.slicer.set_remaining_bytes(remaining_bytes);
            self.slicer.set_bytes_processed(bytes_processed);
            self.slicer.set_bytes_overlapped(bytes_overlapped);
        }

        self.writer.finish()?;

        info!("Done converting flf to parquet in single-threaded mode!");

        if self.slicer.bytes_overlapped() > 0 {
            info!(
                "We read {} bytes two times (due to sliding window overlap).",
                self.slicer.bytes_overlapped(),
            );
        }

        Ok(())
    }

    /// Divide the current buffer into chunks for each worker thread based on the line breaks.
    /// 
    /// # Note
    /// The workload will attempt to be uniform on each worker, however, the worker with the
    /// last index might get some extra lines to process due to number of rows now being
    /// divisible by the estimated number of rows per thread.
    fn distribute_worker_thread_workloads(
        &self,
        line_break_indices: &[usize],
        thread_workloads: &mut Vec<(usize, usize)>,
    ) {
        let n_line_break_indices: usize = line_break_indices.len();
        let n_rows_per_thread: usize = n_line_break_indices / thread_workloads.capacity(); // usize division will floor the result

        let mut prev_line_break_idx: usize = 0;
        for worker_idx in 1..(thread_workloads.capacity()) {
            let next_line_break_idx: usize = n_rows_per_thread * worker_idx;
            thread_workloads.push((prev_line_break_idx, next_line_break_idx));
            prev_line_break_idx = next_line_break_idx;
        }

        thread_workloads.push((prev_line_break_idx, n_line_break_indices))
    }

    ///
    fn spawn_threads(
        &mut self,
        buffer: &Vec<u8>,
        line_breaks: &[usize],
        thread_workloads: &[(usize, usize)],
    ) -> Result<()> {
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);
        let arc_buffer: Arc<&Vec<u8>> = Arc::new(buffer);
        let arc_slicer: Arc<&FileSlicer> = Arc::new(&self.slicer);

        let _ = scope(|s| {
            #[cfg(debug_assertions)]
            debug!("Starting {} worker threads for convertion.", thread_workloads.len());

            let threads = thread_workloads
            .iter()
            .map(|(from, to)| {
                let t_sender: channel::Sender<RecordBatch> = sender.clone();
                let t_line_breaks: &[usize] = &line_breaks[*from..*to];
                let t_buffer: Arc<&Vec<u8>> = arc_buffer.clone();
                let t_slicer: Arc<&FileSlicer> = arc_slicer.clone();

                // Can we do this in another way? So we don't have to allocate a bunch of stuff in our loop...
                let mut t_builders: ParquetBuilder = self.schema.clone().into_builder();
                s.spawn(move |_| {
                    todo!()
                })
            })
            .collect::<Vec<ScopedJoinHandle<()>>>();
        });

        Ok(())
    }
}

impl Converter for ParquetConverter {}

/// A helper struct for building an instance of a [`ParquetConverter`] struct.
#[derive(Default)]
pub struct ParquetConverterBuilder {
    in_path: Option<PathBuf>,
    schema_path: Option<PathBuf>,
    out_path: Option<PathBuf>,
    n_threads: Option<usize>,
    read_buffer_size: Option<usize>,
    thread_channel_capacity: Option<usize>,
    write_properties: Option<ArrowWriterProperties>,
}

impl ParquetConverterBuilder {
    /// Set the relative or absolute path to the input file to convert.
    pub fn with_in_file(mut self, in_path: PathBuf) -> Self {
        self.in_path = Some(in_path);
        self
    }

    /// Set the relative or absolute path to the json schema file to use.
    pub fn with_schema(mut self, schema_path: PathBuf) -> Self {
        self.schema_path = Some(schema_path);
        self
    }

    /// Set the relative or absolute path to the output file to produce.
    pub fn with_out_file(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    /// Set the number of threads (logical cores) to use.
    pub fn with_num_threads(mut self, n_threads: usize) -> Self {
        self.n_threads = Some(n_threads);
        self
    }

    /// Set the buffer size for reading the input file (in bytes).
    pub fn with_read_buffer_size(mut self, buffer_size: usize) -> Self {
        self.read_buffer_size = Some(buffer_size);
        self
    }

    /// Set the properties of the [`ArrowWriter`] which writes to parquet.
    pub fn with_write_properties(mut self, properties: ArrowWriterProperties) -> Self {
        self.write_properties = Some(properties);
        self
    }

    /// Set the maximum message capacity on the multithreaded converter thread channels.
    /// See https://docs.rs/crossbeam/latest/crossbeam/channel/fn.bounded.html for specifics.
    pub fn with_thread_channel_capacity(mut self, thread_channel_capacity: Option<usize>) -> Self {
        self.thread_channel_capacity = thread_channel_capacity;
        self
    }

    /// Try creating a new [`ParquetConverterProperties`] from the previously set values.
    ///
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If any of the required fields are `None`.
    /// * If the schema deserialization failed.
    /// * If any I/O error occured when trying to open the input and output files.
    /// * If a [`SerializedFileWriter`] could not be created from the output file.
    /// * If the [`ArrowSchemaRef`] contains unsupported datatypes.
    ///
    /// [`SerializedFileWriter`]: parquet::file::writer::SerializedFileWriter
    pub fn try_build(self) -> Result<ParquetConverter> {
        let in_file: PathBuf = self.in_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'in_path' was not provided, exiting...",
            ))
        })?;

        let schema_path: PathBuf = self.schema_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'schema_path' was not provided, exiting...",
            ))
        })?;

        let out_path: PathBuf = self.out_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'out_path' was not provided, exiting...",
            ))
        })?;

        let n_threads: usize = self.n_threads.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'n_threads' was not provided, exiting...",
            ))
        })?;

        let read_buffer_size: usize = self.read_buffer_size.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'read_buffer_size' was not provided, exiting...",
            ))
        })?;

        let thread_channel_capacity: usize =
            self.thread_channel_capacity.or(Some(estimate_best_thread_channel_capacity(n_threads))).unwrap();

        let slicer: FileSlicer = FileSlicer::try_from_path(in_file)?;
        let schema: FixedSchema = FixedSchema::from_path(schema_path)?;

        // Here it is okay to clone the entire struct, since this is not executed
        // during any heavy workload, and should only happen during setup.
        let builder: ParquetBuilder = schema.clone().into_builder::<ParquetBuilder>();
        let arrow_schema: ArrowSchemaRef = Arc::new(schema.clone().into_arrow_schema());

        let writer: ParquetWriter = ParquetWriter::builder()
            .with_out_path(out_path)
            .with_arrow_schema(arrow_schema)
            .with_properties(self.write_properties)
            .try_build()?;

        Ok(ParquetConverter {
            slicer,
            writer,
            builder,
            schema,
            read_buffer_size,
            n_threads,
            thread_channel_capacity,
        })
    }
}
