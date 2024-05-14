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
// File created: 2024-02-17
// Last updated: 2024-05-14
//

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use crossbeam::channel;
#[cfg(not(feature = "rayon"))]
use crossbeam::scope;
#[cfg(not(feature = "rayon"))]
use crossbeam::thread::ScopedJoinHandle;
use libc;
#[cfg(debug_assertions)]
use log::debug;
use log::{error, info};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
#[cfg(feature = "rayon")]
use rayon::iter::*;

use std::fs::File;
use std::io::{BufReader, Read};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use crate::builder::ColumnBuilder;
use crate::error::{Result, SetupError};
use crate::mocker::NUM_CHARS_FOR_NEWLINE;
use crate::schema::FixedSchema;
use crate::slicer::Slicer;
use crate::writer::{ParquetWriter, Writer};

///
pub(crate) static CONVERTER_SLICE_BUFFER_SIZE: usize = 8 * 1024 * 1024 * 1024;
///
pub(crate) static CONVERTER_THREAD_CHANNEL_CAPACITY: usize = 128;
///
pub(crate) static CONVERTER_LINE_BREAKS_BUFFER_SIZE: usize = 1 * 1024 * 1024;

///
#[derive(Debug)]
pub struct Converter {
    in_file: File,
    schema: FixedSchema,
    writer: Box<dyn Writer>,
    slicer: Slicer,
    n_threads: usize,
    multithreaded: bool,
    buffer_size: usize,
    thread_channel_capacity: usize,
}

unsafe impl Send for Converter {}
unsafe impl Sync for Converter {}

///
impl Converter {
    /// Create a new instance of a [`Converter`] struct with default values.
    pub fn builder() -> ConverterBuilder {
        ConverterBuilder {
            ..Default::default()
        }
    }

    ///
    pub fn convert(&mut self) -> Result<()> {
        if self.multithreaded {
            info!(
                "Converting in multithreaded mode using {} threads.",
                self.n_threads
            );
            self.convert_multithreaded()?
        } else {
            info!("Converting in single-threaded mode.");
            self.convert_single_threaded()?
        }

        Ok(())
    }

    ///
    /// # Panics
    /// If could not move cursor back.
    fn convert_multithreaded(&mut self) -> Result<()> {
        let bytes_to_read: usize = self.in_file.metadata()?.len() as usize;

        info!(
            "Target file to convert is {} bytes in total.",
            bytes_to_read
        );

        let mut remaining_bytes: usize = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut bytes_overlapped: usize = 0;
        let mut buffer_capacity = self.buffer_size;
        let mut ratio_processed: f32;

        let mut reader: BufReader<File> = BufReader::new(self.in_file.try_clone()?);
        let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];

        let mut line_break_indices: Vec<usize> =
            Vec::with_capacity(CONVERTER_LINE_BREAKS_BUFFER_SIZE);

        let mut worker_line_break_indices: Vec<(usize, usize)> =
            Vec::with_capacity(self.n_threads - 1);

        // Start processing the target file to convert.
        loop {
            if bytes_processed >= bytes_to_read {
                break;
            }

            if remaining_bytes < buffer_capacity {
                buffer_capacity = remaining_bytes;
            }

            #[cfg(debug_assertions)]
            debug!("(UNSAFE) clearing read buffer memory.");
            unsafe {
                libc::memset(
                    buffer.as_mut_ptr() as _,
                    0,
                    buffer.capacity() * mem::size_of::<u8>(),
                );
            }

            match reader.read_exact(&mut buffer).is_ok() {
                true => (),
                false => {
                    #[cfg(debug_assertions)]
                    debug!("EOF reached, this should be the last time reading the buffer.");
                }
            }

            self.slicer
                .find_line_breaks(&buffer, &mut line_break_indices);

            let byte_idx_last_line_break = line_break_indices
                .last()
                .ok_or("No line breaks found in the read buffer!")?;
            let n_bytes_left_after_line_break = buffer_capacity - 1 - byte_idx_last_line_break;

            reader.seek_relative(-(n_bytes_left_after_line_break as i64))?;

            self.distribute_worker_thread_workloads(
                &line_break_indices,
                &mut worker_line_break_indices,
            );

            self.spawn_convert_threads(&buffer, &line_break_indices, &worker_line_break_indices)?;

            bytes_processed += buffer_capacity - n_bytes_left_after_line_break;
            bytes_overlapped += n_bytes_left_after_line_break;
            remaining_bytes -= buffer_capacity - n_bytes_left_after_line_break;
            ratio_processed = 100.0 * bytes_processed as f32 / bytes_to_read as f32;

            line_break_indices.clear();
            worker_line_break_indices.clear();

            #[cfg(debug_assertions)]
            debug!("Bytes processed: {}", bytes_processed);
            #[cfg(debug_assertions)]
            debug!("Remaining bytes: {}", remaining_bytes);

            info!("Estimated progress: {:.1}%", ratio_processed);
        }

        #[cfg(debug_assertions)]
        debug!("Finishing and closing writer.");
        self.writer.finish()?;

        #[cfg(feature = "rayon")]
        info!(
            "Done converting with rayon parallelism using {} threads!",
            self.n_threads
        );

        #[cfg(not(feature = "rayon"))]
        info!(
            "Done converting in standard multithreaded mode using {} threads!",
            self.n_threads
        );
        if bytes_overlapped > 0 {
            info!(
                "We read {} bytes two times (due to sliding window overlap).",
                bytes_overlapped
            );
        }

        Ok(())
    }

    #[cfg(feature = "rayon")]
    fn spawn_convert_threads(
        &mut self,
        buffer: &Vec<u8>,
        line_breaks: &Vec<usize>,
        thread_workloads: &Vec<(usize, usize)>,
    ) -> Result<()> {
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);
        let arc_buffer = Arc::new(buffer);
        let arc_slicer = Arc::new(&self.slicer);

        #[cfg(debug_assertions)]
        debug!("Starting {} worker threads.", thread_workloads.len());

        thread_workloads.into_par_iter().for_each(|(from, to)| {
            let t_sender = sender.clone();
            let t_buffer = arc_buffer.clone();
            let t_line_breaks: &[usize] = &line_breaks[*from..*to];
            let t_slicer = arc_slicer.clone();
            // Note: this allocated new memory on the heap for the builders.
            let mut t_builders = self.schema.as_column_builders();
            worker_thread_convert(
                t_sender,
                &t_buffer,
                t_line_breaks,
                &t_slicer,
                &mut t_builders,
            );
        });

        drop(sender);

        #[cfg(debug_assertions)]
        debug!("Starting master writer thread.");
        master_thread_write(receiver, &mut self.writer)?;

        Ok(())
    }

    /// We use scoped threads and associated handles in this implementation.
    ///
    /// https://docs.rs/crossbeam/latest/crossbeam/fn.scope.html
    /// https://docs.rs/crossbeam/latest/crossbeam/thread/struct.ScopedJoinHandle.html
    #[cfg(not(feature = "rayon"))]
    fn spawn_convert_threads(
        &mut self,
        buffer: &Vec<u8>,
        line_breaks: &Vec<usize>,
        thread_workloads: &Vec<(usize, usize)>,
    ) -> Result<()> {
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);
        let arc_buffer: Arc<&Vec<u8>> = Arc::new(buffer);
        let arc_slicer: Arc<&Slicer> = Arc::new(&self.slicer);

        let _ = scope(|s| {
            #[cfg(debug_assertions)]
            debug!("Starting {} worker threads.", thread_workloads.len());
            let threads = thread_workloads
                .into_iter()
                .map(|(from, to)| {
                    let t_sender: channel::Sender<RecordBatch> = sender.clone();
                    let t_line_breaks: &[usize] = &line_breaks[*from..*to];
                    let t_buffer: Arc<&Vec<u8>> = arc_buffer.clone();
                    let t_slicer: Arc<&Slicer> = arc_slicer.clone();
                    let mut t_builders = self.schema.as_column_builders();
                    s.spawn(move |_| {
                        worker_thread_convert(
                            t_sender,
                            &t_buffer,
                            t_line_breaks,
                            &t_slicer,
                            &mut t_builders,
                        );
                    })
                })
                .collect::<Vec<ScopedJoinHandle<()>>>();

            drop(sender);

            #[cfg(debug_assertions)]
            debug!("Starting master writer thread.");
            master_thread_write(receiver, &mut self.writer).expect("Master writer thread failed!");

            for handle in threads {
                handle.join().expect("Could not join worker thread handle!");
            }
        });

        Ok(())
    }

    /// Converts the target file in single-threaded mode.
    ///
    /// # Panics
    /// Thread can panic for the following reasons:
    ///  - If the file descriptor could not read metadata of the target file.
    ///  - If there existed no line break characters in the buffer that [`BufReader`] writes to.
    ///  - If the [`BufReader`] was not able to move the read cursor back relatively.
    ///
    /// # Unsafe
    /// We use [`libc::memset`] to directly write to and 'reset' the buffer in memory.
    /// This can cause a Segmentation fault if e.g. the memory is not allocated properly,
    /// or we are trying to write outside of the allocated memory area.
    fn convert_single_threaded(&mut self) -> Result<()> {
        let bytes_to_read: usize = self.in_file.metadata()?.len() as usize;

        info!(
            "Target file to convert is {} bytes in total.",
            bytes_to_read
        );

        let mut remaining_bytes: usize = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut bytes_overlapped: usize = 0;
        let mut buffer_capacity = self.buffer_size;

        // We wrap the file descriptor in a [`BufReader`] to improve the syscall
        // efficiency of small and repeated I/O calls to the same file.
        let mut reader: BufReader<&File> = BufReader::new(&self.in_file);
        let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];

        let mut line_break_indices: Vec<usize> =
            Vec::with_capacity(CONVERTER_LINE_BREAKS_BUFFER_SIZE);
        let mut builders: Vec<Box<dyn ColumnBuilder>> = self.schema.as_column_builders();

        // Here we start processing the target file.
        loop {
            if bytes_processed >= bytes_to_read {
                break;
            }

            if remaining_bytes < buffer_capacity {
                buffer_capacity = remaining_bytes;
            }

            #[cfg(debug_assertions)]
            debug!("(UNSAFE) clearing read buffer memory.");
            unsafe {
                libc::memset(
                    buffer.as_mut_ptr() as _,
                    0,
                    buffer.capacity() * mem::size_of::<u8>(),
                );
            }

            match reader.read_exact(&mut buffer).is_ok() {
                true => (),
                false => {
                    #[cfg(debug_assertions)]
                    debug!("EOF reached, this should be the last slice to convert.");
                }
            }

            self.slicer
                .find_line_breaks(&buffer, &mut line_break_indices);
            let byte_idx_last_line_break = line_break_indices
                .last()
                .ok_or("No line breaks found in read buffer!")?;
            let n_bytes_left_after_line_break = buffer_capacity - 1 - byte_idx_last_line_break;

            reader.seek_relative(-(n_bytes_left_after_line_break as i64))?;

            bytes_processed += buffer_capacity - n_bytes_left_after_line_break;
            bytes_overlapped += n_bytes_left_after_line_break;
            remaining_bytes -= buffer_capacity - n_bytes_left_after_line_break;

            let mut prev_line_break_idx: usize = 0;
            for line_break_idx in line_break_indices.iter() {
                let line: &[u8] = &buffer[prev_line_break_idx..*line_break_idx];
                let mut prev_byte_idx: usize = 0;

                for builder in builders.iter_mut() {
                    let byte_idx: usize = self
                        .slicer
                        .find_num_bytes_for_num_runes(&line[prev_byte_idx..], builder.runes());

                    let next_byte_idx: usize = prev_byte_idx + byte_idx;

                    builder.parse_and_push_bytes(&line[prev_byte_idx..next_byte_idx]);
                    prev_byte_idx = next_byte_idx;
                }
                prev_line_break_idx = *line_break_idx + NUM_CHARS_FOR_NEWLINE;
            }

            line_break_indices.clear();

            // NOTE: THIS REALLOCATES THE MEMORY ON THE HEAP!
            let mut builder_write_buffer: Vec<(&str, ArrayRef)> = vec![];

            for builder in builders.iter_mut() {
                builder_write_buffer.push(builder.finish());
            }

            let batch = RecordBatch::try_from_iter(builder_write_buffer)?;

            self.writer.write_batch(&batch)?;

            #[cfg(debug_assertions)]
            debug!("Bytes processed: {}", bytes_processed);
            #[cfg(debug_assertions)]
            debug!("Remaining bytes: {}", remaining_bytes);
        }

        self.writer.finish()?;

        info!("Done converting in single-threaded mode!");
        info!(
            "We read {} bytes two times (due to sliding window overlap).",
            bytes_overlapped
        );

        Ok(())
    }

    fn distribute_worker_thread_workloads(
        &self,
        line_break_indices: &Vec<usize>,
        thread_workloads: &mut Vec<(usize, usize)>,
    ) {
        let n_line_break_indices: usize = line_break_indices.len();
        let n_rows_per_thread: usize = n_line_break_indices / (self.n_threads - 1);

        let mut prev_line_break_idx: usize = 0;
        for worker_idx in 1..(self.n_threads - 1) {
            let next_line_break_idx: usize = n_rows_per_thread * worker_idx;
            thread_workloads.push((prev_line_break_idx, next_line_break_idx));
            prev_line_break_idx = next_line_break_idx;
        }

        thread_workloads.push((prev_line_break_idx, n_line_break_indices));
    }
}

///
pub fn worker_thread_convert(
    channel: channel::Sender<RecordBatch>,
    slice: &[u8],
    line_breaks: &[usize],
    slicer: &Slicer,
    builders: &mut Vec<Box<dyn ColumnBuilder>>,
) {
    let mut prev_line_break_idx: usize = 0;

    for line_break_idx in line_breaks.iter() {
        let line: &[u8] = &slice[prev_line_break_idx..*line_break_idx];
        let mut prev_byte_idx: usize = 0;
        for builder in builders.iter_mut() {
            let byte_idx: usize =
                slicer.find_num_bytes_for_num_runes(&line[prev_byte_idx..], builder.runes());

            let next_byte_idx: usize = prev_byte_idx + byte_idx;
            builder.parse_and_push_bytes(&line[prev_byte_idx..next_byte_idx]);
            prev_byte_idx = next_byte_idx;
        }
        prev_line_break_idx = line_break_idx + NUM_CHARS_FOR_NEWLINE;
    }

    // Note: again we allocated memory on the heap from each thread, bad!
    let mut builder_write_buffer: Vec<(&str, ArrayRef)> = vec![];

    for builder in builders.iter_mut() {
        builder_write_buffer.push(builder.finish());
    }

    channel
        .send(RecordBatch::try_from_iter(builder_write_buffer).unwrap())
        .unwrap();

    drop(channel);
}

///
pub fn master_thread_write(
    channel: channel::Receiver<RecordBatch>,
    writer: &mut Box<dyn Writer>,
) -> Result<()> {
    for record_batch in channel {
        writer.write_batch(&record_batch)?;
        drop(record_batch);
    }

    #[cfg(debug_assertions)]
    debug!("Master thread done writing converted slice!");

    Ok(())
}

///
#[derive(Debug, Default)]
pub struct ConverterBuilder {
    in_file: Option<PathBuf>,
    out_file: Option<PathBuf>,
    schema_file: Option<PathBuf>,
    n_threads: Option<usize>,
    multithreaded: Option<bool>,
    buffer_size: Option<usize>,
    thread_channel_capacity: Option<usize>,
}

///
impl ConverterBuilder {
    /// Set the [`PathBuf`] for the target file to convert.
    pub fn with_in_file(mut self, in_file: PathBuf) -> Self {
        self.in_file = Some(in_file);
        self
    }

    /// Set the [`PathBuf`] for the output file of the converted data.
    pub fn with_out_file(mut self, out_file: PathBuf) -> Self {
        self.out_file = Some(out_file);
        self
    }

    /// Set the [`PathBuf`] for the schema of the target file.
    pub fn with_schema(mut self, schema_file: PathBuf) -> Self {
        self.schema_file = Some(schema_file);
        self
    }

    /// Set the number of threads to use when converting the target file with the [`Converter`].
    pub fn with_num_threads(mut self, n_threads: usize) -> Self {
        self.n_threads = Some(n_threads);
        self.multithreaded = Some(n_threads > 1);
        self
    }

    ///
    pub fn with_buffer_size(mut self, buffer_size: Option<usize>) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    ///
    pub fn with_thread_channel_capacity(mut self, thread_channel_capacity: Option<usize>) -> Self {
        self.thread_channel_capacity = thread_channel_capacity;
        self
    }

    /// Verify that all required fields have been set approriately and then
    /// create a new [`Converter`] instance based on the provided fields.
    /// Any optional fields not provided will be set according to either
    /// random strategies or assigned global static variables.
    ///
    /// # Error
    /// Iff any of the required fields are `None`.
    ///
    /// # Panics
    /// ...
    pub fn build(self) -> Result<Converter> {
        let in_file: File = match self.in_file {
            Some(p) => File::open(p)?,
            None => {
                error!("Required field 'in_file' is missing or None.");
                return Err(Box::new(SetupError));
            }
        };

        let out_file: PathBuf = self
            .out_file
            .ok_or("Required field 'out_file' is missing or None.")?;

        let schema: FixedSchema = match self.schema_file {
            Some(p) => FixedSchema::from_path(p)?,
            None => {
                error!("Required field 'schema_file' is missing or None.");
                return Err(Box::new(SetupError));
            }
        };

        let n_threads: usize = self
            .n_threads
            .ok_or("Required field 'n_threads' is missing or None.")?;

        let multithreaded: bool = self
            .multithreaded
            .ok_or("Required field 'multithreaded' is missing or None.")?;

        //
        // Optional configuration below.
        //
        let buffer_size: usize = match self.buffer_size {
            Some(s) => {
                if multithreaded {
                    s / (n_threads - 1)
                } else {
                    s
                }
            }
            None => {
                if multithreaded {
                    #[cfg(debug_assertions)]
                    debug!(
                        "Optional field `buffer_size` not provided, will use static value CONVERTER_SLICE_BUFFER_SIZE={}.",
                        CONVERTER_SLICE_BUFFER_SIZE / (n_threads - 1),
                    );
                    CONVERTER_SLICE_BUFFER_SIZE / (n_threads - 1)
                } else {
                    #[cfg(debug_assertions)]
                    debug!(
                        "Optional field `buffer_size` not provided, will use static value CONVERTER_SLICE_BUFFER_SIZE={}.",
                        CONVERTER_SLICE_BUFFER_SIZE / (n_threads - 1),
                    );
                    CONVERTER_SLICE_BUFFER_SIZE / (n_threads - 1)
                }
            }
        };

        let thread_channel_capacity: usize = match self.thread_channel_capacity {
            Some(c) => c,
            None => {
                #[cfg(debug_assertions)]
                debug!(
                    "Optional field `thread_channel_capacity` not provided, will use static value CONVERTER_THREAD_CHANNEL_CAPACITY={}.",
                      CONVERTER_THREAD_CHANNEL_CAPACITY,
                );
                CONVERTER_THREAD_CHANNEL_CAPACITY
            }
        };

        let properties: ParquetWriterProperties = ParquetWriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let arrow_schema: ArrowSchemaRef = schema.as_arrow_schema_ref();

        let writer: Box<dyn Writer> = Box::new(
            ParquetWriter::builder()
                .with_out_file(out_file)
                .with_properties(properties)
                .with_arrow_schema(arrow_schema)
                .build()?,
        );

        let slicer = Slicer::builder().num_threads(n_threads).build()?;

        Ok(Converter {
            in_file,
            schema,
            writer,
            slicer,
            n_threads,
            multithreaded,
            buffer_size,
            thread_channel_capacity,
        })
    }
}
