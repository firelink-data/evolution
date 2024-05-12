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
// Last updated: 2024-05-11
//

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use libc;
use log::{debug, error, info};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use std::fs::File;
use std::io::{BufReader, Read};
use std::mem;
use std::path::PathBuf;

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
    output_path: PathBuf,
    target_file: File,
    schema: FixedSchema,
    n_threads: usize,
    multithreading: bool,
    buffer_size: usize,
    thread_channel_capacity: usize,
    slicer: Slicer,
}

///
impl Converter {
    /// Create a new instance of a [`Converter`] struct with default values.
    pub fn builder() -> ConverterBuilder {
        ConverterBuilder {
            ..Default::default()
        }
    }

    ///
    pub fn convert(&mut self) {
        if self.multithreading {
            info!(
                "Converting in multithreaded mode using {} threads.",
                self.n_threads
            );
            self.convert_multithreaded()
        } else {
            info!("Converting in single-threaded mode.");
            self.convert_single_threaded()
        }
    }

    ///
    fn convert_multithreaded(&mut self) {
        todo!();
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
    fn convert_single_threaded(&mut self) {
        let bytes_to_read: usize = self
            .target_file
            .metadata()
            .expect("Could not read file metadata!")
            .len() as usize;

        info!("Target file is {} bytes in total.", bytes_to_read);

        let mut remaining_bytes: usize = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut bytes_overlapped: usize = 0;
        let mut buffer_capacity = self.buffer_size;

        // We wrap the file descriptor in a [`BufReader`] to improve the syscall
        // efficiency of small and repeated I/O calls to the same file.
        let mut reader: BufReader<&File> = BufReader::new(&self.target_file);
        let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];

        let mut line_break_indices: Vec<usize> =
            Vec::with_capacity(CONVERTER_LINE_BREAKS_BUFFER_SIZE);
        let mut builders: Vec<Box<dyn ColumnBuilder>> = self.schema.as_column_builders();

        // Setup writer configuration for specific target schema.
        let mut empty_column_builders = self
            .schema
            .iter()
            .map(|col| col.as_column_builder())
            .collect::<Vec<Box<dyn ColumnBuilder>>>();

        let column_refs = empty_column_builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Vec<(&str, ArrayRef)>>();

        let writer_record_batch = RecordBatch::try_from_iter(column_refs)
            .expect("Could not create RecordBatch from FixedSchema columns!");

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        debug!("Creating and setting up ParquetWriter.");
        let mut writer: ParquetWriter = ParquetWriter::new(&self.output_path);
        writer.set_properties(writer_properties);
        writer.set_record_batch(writer_record_batch);
        writer.setup_arrow_writer();

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

            writer.write_batch(&batch);

            debug!("Bytes processed: {}", bytes_processed);
            debug!("Remaining bytes: {}", remaining_bytes);
        }

        writer.finish();

        info!(
            "Done converting! We read {} bytes two times (due to sliding window overlap).",
            bytes_overlapped,
        );
    }
}

///
#[derive(Debug, Default)]
pub struct ConverterBuilder {
    target_file_path: Option<PathBuf>,
    output_file_path: Option<PathBuf>,
    schema_path: Option<PathBuf>,
    n_threads: Option<usize>,
    multithreading: Option<bool>,
    buffer_size: Option<usize>,
    thread_channel_capacity: Option<usize>,
}

///
impl ConverterBuilder {
    /// Set the [`PathBuf`] for the target file to convert.
    pub fn target_file(mut self, file_path: PathBuf) -> Self {
        self.target_file_path = Some(file_path);
        self
    }

    /// Set the [`PathBuf`] for the output file of the converted data.
    pub fn output_file(mut self, file_path: PathBuf) -> Self {
        self.output_file_path = Some(file_path);
        self
    }

    /// Set the [`PathBuf`] for the schema of the target file.
    pub fn schema(mut self, schema_path: PathBuf) -> Self {
        self.schema_path = Some(schema_path);
        self
    }

    /// Set the number of threads to use when converting the target file with the [`Converter`].
    pub fn num_threads(mut self, n_threads: usize) -> Self {
        self.n_threads = Some(n_threads);
        self.multithreading = Some(n_threads > 1);
        self
    }

    ///
    pub fn buffer_size(mut self, buffer_size: Option<usize>) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    ///
    pub fn thread_channel_capacity(mut self, thread_channel_capacity: Option<usize>) -> Self {
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
    pub fn build(self) -> Result<Converter> {
        let target_file: File = match self.target_file_path {
            Some(p) => match File::open(&p) {
                Ok(f) => f,
                Err(e) => {
                    error!("Could not open target file path, reason: {}", e);
                    return Err(Box::new(SetupError));
                }
            },
            None => {
                error!("Required field target_file_path not provided, exiting...");
                return Err(Box::new(SetupError));
            }
        };

        let output_path: PathBuf = match self.output_file_path {
            Some(p) => p,
            None => {
                error!("Required field target_file_path not provided, exiting...");
                return Err(Box::new(SetupError));
            }
        };

        // The call to [`FixedSchema::from_path`] might panic.
        let schema: FixedSchema = match self.schema_path {
            Some(path) => FixedSchema::from_path(path),
            None => {
                error!("Required field `schema_path` not provided, exiting...");
                return Err(Box::new(SetupError));
            }
        };

        let n_threads: usize = match self.n_threads {
            Some(n) => n,
            None => {
                error!("Required field `n_threads` not provided, exiting...");
                return Err(Box::new(SetupError));
            }
        };

        let multithreading: bool = match self.multithreading {
            Some(b) => b,
            None => {
                error!("Required field `multithreading` not provided, exiting...");
                return Err(Box::new(SetupError));
            }
        };

        //
        // Optional configuration below.
        //
        let buffer_size: usize = match self.buffer_size {
            Some(s) => {
                if multithreading {
                    s / (n_threads - 1)
                } else {
                    s
                }
            }
            None => {
                if multithreading {
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

        let slicer = Slicer::builder().num_threads(n_threads).build()?;

        Ok(Converter {
            output_path,
            target_file,
            schema,
            n_threads,
            multithreading,
            buffer_size,
            thread_channel_capacity,
            slicer,
        })
    }
}
