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
// File created: 2024-10-19
// Last updated: 2024-10-19
//

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use bytesize::ByteSize;
use crossbeam::channel;
use crossbeam::thread::scope;
use crossbeam::thread::ScopedJoinHandle;
use parquet::file::properties::WriterProperties as ArrowWriterProperties;

use evolution_builder::builder::Builder;
use evolution_builder::parquet::ParquetBuilder;
use evolution_common::error::{ExecutionError, Result, SetupError};
use evolution_common::thread::estimate_best_thread_channel_capacity;
use evolution_common::NUM_BYTES_FOR_NEWLINE;
use evolution_schema::schema::FixedSchema;
use evolution_slicer::slicer::SlicerRef;
use evolution_slicer::fixed_width::FixedWidthSlicer;
use evolution_writer::parquet::ParquetWriter;

#[cfg(debug_assertions)]
use log::debug;
use log::info;

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crate::converter::Converter;

/// Struct for converting any fixed-length file into the parquet file format.
pub struct ParquetConverter<'a, B, W> {
    slicer: SlicerRef<'a, B, W>,
    writer: ParquetWriter,
    builder: ParquetBuilder,
    schema: FixedSchema,
    // The size of the buffer that reads the input file (in bytes).
    read_buffer_size: usize,
    // The number of threads to use when converting.
    n_threads: usize,
    // The maximum number of active messages allowed in the thread channels.
    thread_channel_capacity: usize,
}

impl<'a, B, W> ParquetConverter<'a, B, W> {
    /// Create a new instance of a [`ParquetConverterBuilder`] with default values.
    pub fn builder() -> ParquetConverterBuilder {
        ParquetConverterBuilder {
            ..Default::default()
        }
    }

    /// Try and convert the target file to parquet format in multithreaded mode.
    ///
    /// # Panics
    /// The multithreading is implemented using [`crossbeam`] scoped threads, and we don't currently support returning
    /// anything from the scope, so any function called inside the scope that returns a [`Result`] has to be unwrapped.
    ///
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If the [`FileSlicer`] fails to read the expected amount of bytes to the buffer.
    /// * If the buffer was empty when trying to find line-breaks in it.
    /// * If the buffer did not contain any line-break characters at all.
    /// * If any of the threading operations returned an Error during the conversion.
    /// * If the [`ParquetWriter`] failed flushing all of the RowGroups to file.
    pub fn try_convert_multithreaded(&mut self) -> Result<()> {
        let mut buffer_capacity = self.read_buffer_size;
        let n_worker_threads: usize = self.n_threads - 1;
        let mut ratio_processed: f32;

        info!("Converting flf to parquet in multithreaded mode.");
        info!(
            "The file to convert is ~{} in total.",
            ByteSize::gb((self.slicer.bytes_to_read() / 1_000_000_000) as u64),
        );

        let mut line_break_indices: Vec<usize> = Vec::with_capacity(buffer_capacity);
        let mut thread_workloads: Vec<(usize, usize)> = Vec::with_capacity(n_worker_threads);

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
            self.slicer
                .try_distribute_workloads(&mut buffer, &mut thread_workloads)?;
            self.spawn_converter_threads(&buffer, &thread_workloads)?;

            let n_bytes_left_after_last_line_break: usize =
                buffer_capacity - thread_workloads[n_worker_threads - 1].1 - NUM_BYTES_FOR_NEWLINE;
            self.slicer
                .try_seek_relative(-(n_bytes_left_after_last_line_break as i64))?;

            bytes_processed += buffer_capacity - n_bytes_left_after_last_line_break;
            bytes_overlapped += n_bytes_left_after_last_line_break;
            remaining_bytes -= buffer_capacity - n_bytes_left_after_last_line_break;
            ratio_processed = 100.0 * bytes_processed as f32 / self.slicer.bytes_to_read() as f32;

            self.slicer.set_remaining_bytes(remaining_bytes);
            self.slicer.set_bytes_processed(bytes_processed);
            self.slicer.set_bytes_overlapped(bytes_overlapped);

            line_break_indices.clear();
            thread_workloads.clear();

            info!("Estimated progress: {:.2}%", ratio_processed);
        }

        #[cfg(debug_assertions)]
        debug!("Finishing and closing writer.");
        self.writer.try_finish()?;

        info!("Done converting flf to parquet in multithreaded mode!");

        if self.slicer.bytes_overlapped() > 0 {
            info!(
                "We read {} bytes two times (due to sliding window overlap).",
                self.slicer.bytes_overlapped(),
            );
        }

        Ok(())
    }

    /// Try and convert the target file to parquet format in single-threaded mode.
    ///
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If the [`FileSlicer`] fails to read the expected amount of bytes to the buffer.
    /// * If the buffer was empty when trying to find line-breaks in it.
    /// * If the buffer did not contain any line-break characters at all.
    /// * If the [`ParquetWriter`] failed flushing all of the RowGroups to file.
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
            //
            // But maybe this is not too bad, the allocations don't take that long compared to the I/O...
            let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];
            self.slicer.try_read_to_buffer(&mut buffer)?;

            let byte_idx_last_line_break: usize = self.slicer.try_find_last_line_break(&buffer)?;
            let n_bytes_left_after_last_line_break: usize =
                buffer_capacity - byte_idx_last_line_break - NUM_BYTES_FOR_NEWLINE;

            self.builder.try_build_from(&buffer)?;
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

        self.writer.try_finish()?;

        info!("Done converting flf to parquet in single-threaded mode!");

        if self.slicer.bytes_overlapped() > 0 {
            info!(
                "We read {} bytes two times (due to sliding window overlap).",
                self.slicer.bytes_overlapped(),
            );
        }

        Ok(())
    }

    /// Spawn the threads which perform the conversion, specifically, n-1 threads will work on converting and 1
    /// thread will collect and buffer the converted results and write those to parquet file, where n was the
    /// specified number of threads for the program.
    ///
    /// # Note
    /// The threading is implemented using [`crossbeam`] and might perform differently depending on host system.
    ///
    /// # Panics
    /// This function will panic and terminate execution for the following reasons:
    /// * If the [`ParquetBuilder`] was unable to parse a column to its expected format.
    /// * If a thread not being able to communicate through the channel due to it being disconnected.
    /// * If the [`ParquetWriter`] could not write the parsed columns as a [`RecordBatch`].
    /// * If any worker thread could not join the main thread from its handle.
    ///
    /// # Errors
    /// If and only if the thread scope closure returned an error, which will propagate an [`ExecutionError`].
    ///
    /// [`RecordBatch`]: arrow::array::RecordBatch
    fn spawn_converter_threads(
        &mut self,
        buffer: &Vec<u8>,
        thread_workloads: &[(usize, usize)],
    ) -> Result<()> {
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);
        let arc_buffer: Arc<&Vec<u8>> = Arc::new(buffer);

        let thread_result: thread::Result<()> = scope(|s| {
            let threads = thread_workloads
                .iter()
                .map(|(from, to)| {
                    let t_sender: channel::Sender<ParquetBuilder> = sender.clone();
                    // Can we do this in another way? So we don't have to allocate a bunch of stuff in our loop...
                    // TODO: pull this out and create them as part of the ParquetConverter struct?..
                    let mut t_builder: ParquetBuilder = self.schema.clone().into_builder();

                    let t_buffer: Arc<&Vec<u8>> = arc_buffer.clone();
                    let t_buffer_slice: &[u8] = &t_buffer[*from..*to];

                    s.spawn(move |_| {
                        t_builder.try_build_from(t_buffer_slice).unwrap();
                        t_sender.send(t_builder).unwrap();
                        drop(t_sender);
                    })
                })
                .collect::<Vec<ScopedJoinHandle<()>>>();

            drop(sender);
            for mut builder in receiver {
                self.writer.try_write_from_builder(&mut builder).unwrap();
                drop(builder);
            }

            for handle in threads {
                handle.join().expect("Could not join worker thread handle!");
            }
        });

        if thread_result.is_err() {
            return Err(Box::new(ExecutionError::new(
                format!(
                    "One of the scoped threads returned an error: {:?}",
                    thread_result
                )
                .as_str(),
            )));
        }

        #[cfg(debug_assertions)]
        debug!("Buffer chunk done!");

        Ok(())
    }
}

impl<B, W> Converter for ParquetConverter<'_, B, W> {
    /// Convert the provided fixed-length file to parquet output format.
    ///
    /// # Panics
    /// This function will panic on any errors created in any of the conversion modes, see any
    /// of the functions [`try_convert_multithreaded`] or [`try_convert_single_threaded`] for specifics.
    ///
    /// [`try_convert_multithreaded`]: ParquetConverter::try_convert_multithreaded
    /// [`try_convert_single_threaded`]: ParquetConverter::try_convert_single_threaded
    fn convert(&mut self) {
        if self.n_threads > 1 {
            self.try_convert_multithreaded().unwrap();
        } else {
            self.try_convert_single_threaded().unwrap();
        }
    }

    /// Try and convert the provided fixed-length file to parquet output format.
    ///
    /// # Errors
    /// This function will propagate any errors created in any of the conversion modes, see any
    /// of the functions [`try_convert_multithreaded`] or [`try_convert_single_threaded`] for specifics.
    ///
    /// [`try_convert_multithreaded`]: ParquetConverter::try_convert_multithreaded
    /// [`try_convert_single_threaded`]: ParquetConverter::try_convert_single_threaded
    fn try_convert(&mut self) -> Result<()> {
        if self.n_threads > 1 {
            self.try_convert_multithreaded()?;
        } else {
            self.try_convert_single_threaded()?;
        }
        Ok(())
    }
}

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
    pub fn with_thread_channel_capacity(mut self, capacity: Option<usize>) -> Self {
        self.thread_channel_capacity = capacity;
        self
    }

    /// Set default values for the optional configuration fields.
    ///
    /// # Note
    /// Default conversion mode is always multithreaded utilizing all available threads (logical cores).
    pub fn with_default_values(mut self) -> Self {
        let n_threads: usize = num_cpus::get();
        self.n_threads = Some(n_threads);
        self.read_buffer_size = Some(500 * 1024 * 1024); // 500 MB
        self.thread_channel_capacity = Some(n_threads);
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

        let thread_channel_capacity: usize = self
            .thread_channel_capacity
            .unwrap_or(estimate_best_thread_channel_capacity(n_threads));

        let slicer: FixedWidthSlicer = FixedWidthSlicer::try_from_path(in_file)?;
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
