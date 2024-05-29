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
// Last updated: 2024-05-30
//

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use evolution_builder::builder::ParquetBuilder;
use evolution_common::error::{ExecutionError, Result, SetupError};
use evolution_common::NUM_BYTES_FOR_NEWLINE;
use evolution_schema::schema::FixedSchema;
use evolution_slicer::slicer::{FileSlicer, Slicer};
use evolution_writer::parquet::ParquetWriter;
use log::info;
use parquet::file::properties::WriterProperties as ArrowWriterProperties;

use std::mem;
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
    n_threads: usize,
    read_buffer_size: usize,
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

    ///
    pub fn try_convert_multithreaded(&mut self) -> Result<()> {
        todo!()
    }

    ///
    pub fn try_convert_single_threaded(&mut self) -> Result<()> {
        let mut buffer_capacity: usize = self.read_buffer_size;
        let mut buffer: Vec<u8> = vec![0u8; buffer_capacity];

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

            // This is to not have to perform a syscall and realloc the buffer.
            // We save precious CPU cycles here, but unsafe... scary!
            unsafe {
                libc::memset(
                    buffer.as_mut_ptr() as _,
                    0,
                    buffer.capacity() * mem::size_of::<u8>(),
                );
            }

            self.slicer.try_read_to_buffer(&mut buffer)?;
            let byte_idx_last_line_break: usize = self.slicer
                .try_find_last_line_break(&buffer)?;

            let n_bytes_left_after_last_line_break: usize =
                buffer_capacity - byte_idx_last_line_break - NUM_BYTES_FOR_NEWLINE;

            self.builder.try_parse_slice(&buffer)?;

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

        let write_properties: ArrowWriterProperties = self.write_properties.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'write_properties' was not provided, exiting...",
            ))
        })?;

        let slicer: FileSlicer = FileSlicer::try_from_path(in_file)?;

        let fixed_schema: FixedSchema = FixedSchema::from_path(schema_path)?;

        // Here it is okay to clone the entire struct, since this is not executed
        // during any heavy workload, and should only happen during setup.
        let builder: ParquetBuilder = fixed_schema.clone()
            .into_builder::<ParquetBuilder>();

        let arrow_schema: ArrowSchemaRef =
            Arc::new(fixed_schema.into_arrow_schema());

        let writer: ParquetWriter = ParquetWriter::builder()
            .with_out_path(out_path)
            .with_arrow_schema(arrow_schema)
            .with_properties(write_properties)
            .try_build()?;

        Ok(ParquetConverter {
            slicer,
            writer,
            builder,
            n_threads,
            read_buffer_size,
        })
    }
}
