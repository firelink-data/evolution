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
// Last updated: 2024-05-13
//

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use crossbeam::channel;
use libc;
use log::{debug, error, info};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
#[cfg(feature = "rayon")]
use rayon::iter::*;

use std::fs::File;
use std::io::{BufReader, Read};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(not(feature = "rayon"))]
use std::thread::spawn;
#[cfg(not(feature = "rayon"))]
use std::thread::JoinHandle;

use crate::builder::ColumnBuilder;
use crate::error::{Result, SetupError};
use crate::mocker::NUM_CHARS_FOR_NEWLINE;
use crate::schema::FixedSchema;
use crate::slicer::Slicer;
use crate::writer::{ParquetWriter, Writer};

///
pub(crate) static CONVERTER_SLICE_BUFFER_SIZE: usize = 2 * 1024 * 1024;
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
        let bytes_to_read: usize = self
            .in_file
            .metadata()?
            .len() as usize;

        info!("Target file to convert is {} bytes in total.", bytes_to_read);

        let mut remaining_bytes: usize = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut bytes_overlapped: usize = 0;
        let mut buffer_capacity = self.buffer_size;

        let mut reader: BufReader<File> = BufReader::new(self.in_file.try_clone()?);
        let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];

        let mut line_break_indices: Vec<usize> =
            Vec::with_capacity(CONVERTER_LINE_BREAKS_BUFFER_SIZE);

        let mut worker_builders = (0..(self.n_threads - 1))
            .map(|_| {
                self.schema.as_column_builders()
            })
            .collect::<Vec<Vec<Box<dyn ColumnBuilder>>>>();

        let mut worker_slice_indices: Vec<(usize, usize)> = Vec::with_capacity(self.n_threads - 1);
        let mut worker_slices: Vec<&[u8]> = Vec::with_capacity(self.n_threads - 1);

        // Start processing the target file to convert.
        loop {
            if bytes_processed >= bytes_to_read {
                break;
            }

            if remaining_bytes < buffer_capacity {
                buffer_capacity = remaining_bytes;
            }

            debug!("(UNSAFE) clearing read buffer.");
            unsafe {
                libc::memset(
                    buffer.as_mut_ptr() as _,
                    0,
                    buffer.capacity() * mem::size_of::<u8>(),
                );
            }

            match reader.read_exact(&mut buffer).is_ok() {
                true => (),
                false => debug!("EOF reached, this is the last time reading the buffer."),
            }

            self.slicer
                .find_line_breaks(&buffer, &mut line_break_indices);

            let byte_idx_last_line_break = line_break_indices
                .last()
                .expect("No line breaks found in the read buffer!");
            let n_bytes_left_after_line_break = buffer_capacity - 1 - byte_idx_last_line_break;

            match reader
                .seek_relative(-(n_bytes_left_after_line_break as i64))
                .is_ok()
            {
                true => {}
                false => panic!("Could not move cursor back in BufReader!"),
            };

            self.distribute_worker_thread_workloads(
                &line_break_indices,
                &mut worker_slice_indices,
            );

            debug!("Line break indices: {:?}", line_break_indices);
            debug!("Worker thread slice indices: {:?}", worker_slice_indices);


            self.spawn_convert_threads(
                &buffer,
                &worker_slice_indices,
                &mut worker_builders,
            )?;

            bytes_processed += buffer_capacity - n_bytes_left_after_line_break;
            bytes_overlapped += n_bytes_left_after_line_break;
            remaining_bytes -= buffer_capacity - n_bytes_left_after_line_break;

            line_break_indices.clear();
            worker_slice_indices.clear();
        }

        info!("We read {} bytes two times (due to sliding window overlap).", bytes_overlapped);

        Ok(())
    }

    #[cfg(feature = "rayon")]
    fn spawn_convert_threads(
        &mut self,
        buffer: &Vec<u8>,
        thread_workloads: &Vec<(usize, usize)>,
        builders: &mut Vec<Vec<Box<dyn ColumnBuilder>>>,
    ) -> Result<()> {
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);

        let arc_buffer = Arc::new(buffer);

        thread_workloads
            .into_par_iter()
            .enumerate()
            .for_each(|(thread, (from, to))| {
                let t_sender = sender.clone();
                let t_buffer = arc_buffer.clone();
                let mut t_builders = &builders[thread];
                dummy_worker(t_sender, thread, &t_buffer[*from..*to], &mut t_builders)
            });

        drop(sender);
        dummy_master(receiver, &mut self.writer)?;

        Ok(())
    }

    /// We can never use [`spawn`] with borrowed data...
    #[cfg(not(feature = "rayon"))]
    fn spawn_convert_threads(
        &mut self,
        buffer: &Vec<u8>,
        thread_workloads: &Vec<(usize, usize)>,
        builders: &mut Vec<Vec<Box<dyn ColumnBuilder>>>,
    ) -> Result<()> {
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);

        let arc_buffer = Arc::new(buffer);

        let threads = thread_workloads
            .into_iter()
            .enumerate()
            .map(|(thread, (from, to))| {
                let t_sender = sender.clone();
                let t_buffer = arc_buffer.clone();
                let mut t_builders = &builders[thread];
                spawn(move || {
                    dummy_worker(t_sender, thread, &t_buffer[*from..*to], &mut t_builders)
                })
            })
            .collect::<Vec<JoinHandle<()>>>();

        drop(sender);
        dummy_master(receiver, &mut self.writer)?;
        
        for handle in threads {
            handle.join().unwrap();
        }

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
        let bytes_to_read: usize = self
            .in_file
            .metadata()?
            .len() as usize;

        info!("Target file to convert is {} bytes in total.", bytes_to_read);

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

            debug!("(UNSAFE) clearing read buffer.");
            unsafe {
                libc::memset(
                    buffer.as_mut_ptr() as _,
                    0,
                    buffer.capacity() * mem::size_of::<u8>(),
                );
            }

            match reader.read_exact(&mut buffer).is_ok() {
                true => (),
                false => debug!("EOF reached, this is the last time reading the buffer."),
            }

            self.slicer
                .find_line_breaks(&buffer, &mut line_break_indices);
            let byte_idx_last_line_break = line_break_indices
                .last()
                .expect("No line breaks found in the read buffer!");
            let n_bytes_left_after_line_break = buffer_capacity - 1 - byte_idx_last_line_break;

            match reader
                .seek_relative(-(n_bytes_left_after_line_break as i64))
                .is_ok()
            {
                true => {}
                false => panic!("Could not move cursor back in BufReader!"),
            };

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

            // IF WE DO THIS WE CAN COMPILE, BUT HERE WE ALLOCATE MEMORY
            // FOR A NEW VEC EVERY ITERATION OF THE LOOP. WE WANT TO REUSE
            // MEMORY PREFERABLY, but can't find a way to do that with the
            // required mutable borrows going on...
            //
            // I think I understand why...
            // Because we allocated the mutable buffer outside of the loop
            // and then push mutably borrowed items from inside the loop
            // the borrow checker sees that we might have access to that
            // mutably borrowed memory after the loop, i.e., two mutable
            // references at the same time. This is not ok.
            //
            // // Create the mutable builders and buffers.
            // let mut builders = Vec::with_capacity();
            // let mut buffer = Vec::with_capacity();
            // loop {
            //     // Mutably borrow the builders in iterator
            //     for builder in builders.iter_mut() {
            //         ...
            //     }
            //
            //     for builder in builders.iter_mut() {
            //         // Try add push mutable reference to buffer outside of the
            //         // scope of the loop, this is not ok!
            //         buffer.push(builder.finish());
            //     }
            //
            // }
            //
            // // Here we still MIGHT have access to the mutable references
            // // created inside the loop.
            // let _ = buffer[0];  <- here we can access mutably borrowed memory
            //
            // NOTE: THIS REALLOCATES THE MEMORY ON THE HEAP!
            let mut builder_write_buffer: Vec<(&str, ArrayRef)> = vec![];

            for builder in builders.iter_mut() {
                builder_write_buffer.push(builder.finish());
            }

            let batch = RecordBatch::try_from_iter(builder_write_buffer)
                .expect("Could not create RecordBatch from finished ArrayRefs!");

            self.writer.write_batch(&batch)?;

            debug!("Bytes processed: {}", bytes_processed);
            debug!("Remaining bytes: {}", remaining_bytes);
        }

        self.writer.finish()?;

        info!(
            "Done converting! We read {} bytes two times (due to sliding window overlap).",
            bytes_overlapped,
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
            let next_line_break_idx: usize = line_break_indices[n_rows_per_thread * worker_idx];
            thread_workloads.push((prev_line_break_idx, next_line_break_idx));
            prev_line_break_idx = next_line_break_idx;
        }

        thread_workloads.push((
            prev_line_break_idx,
            line_break_indices[n_line_break_indices - 1],
        ));
    }
}

pub fn dummy_worker(
    channel: channel::Sender<(&str, ArrayRef)>,
    thread: usize,
    slice: &[u8],
    builders: &Vec<Box<dyn ColumnBuilder>>,
) {
    debug!("Hello from worker thread {}.", thread);
}

pub fn dummy_master(
    channel: channel::Receiver<(&str, ArrayRef)>,
    writer: &mut Box<dyn Writer>,
) -> Result<()> {
    debug!("Hello from master thread.");
    Ok(())
}

///
pub fn worker_thread_convert<'a>(
    _channel: channel::Sender<(&str, ArrayRef)>,
    _thread: usize,
    _slice: &'a [u8],
    _builders: &'a Vec<Box<dyn ColumnBuilder>>,
) {
    todo!()
}

///
pub fn master_thread_write() -> Result<()> {
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
            },
        };

        let out_file: PathBuf = self.out_file
            .ok_or("Required field 'out_file' is missing or None.")?;

        let schema: FixedSchema = match self.schema_file {
            Some(p) => FixedSchema::from_path(p)?,
            None => {
                error!("Required field 'schema_file' is missing or None.");
                return Err(Box::new(SetupError));
            },
        };

        let n_threads: usize = self.n_threads
            .ok_or("Required field 'n_threads' is missing or None.")?;

        let multithreaded: bool = self.multithreaded
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
                    info!(
                        "Optional field `buffer_size` not provided, will use static value CONVERTER_SLICE_BUFFER_SIZE={}.",
                        CONVERTER_SLICE_BUFFER_SIZE / (n_threads - 1),
                    );
                    CONVERTER_SLICE_BUFFER_SIZE / (n_threads - 1)
                } else {
                    info!(
                        "Optional field `buffer_size` not provided, will use static value CONVERTER_SLICE_BUFFER_SIZE={}.",
                        CONVERTER_SLICE_BUFFER_SIZE,
                    );
                    CONVERTER_SLICE_BUFFER_SIZE
                }
            }
        };

        let thread_channel_capacity: usize = match self.thread_channel_capacity {
            Some(c) => c,
            None => {
                info!(
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

        let writer: Box<dyn Writer> = Box::new(ParquetWriter::builder()
            .with_out_file(out_file)
            .with_properties(properties)
            .with_arrow_schema(arrow_schema)
            .build()?);

        let slicer = Slicer::builder()
            .num_threads(n_threads)
            .build()?;

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
