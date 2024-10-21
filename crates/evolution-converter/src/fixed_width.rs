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
// Last updated: 2024-10-21
//

use bytesize::ByteSize;
use evolution_builder::builder::BuilderRef;
use evolution_common::NUM_BYTES_FOR_NEWLINE;
use evolution_common::error::{Result, SetupError};
use evolution_schema::schema::FixedWidthSchema;
use evolution_slicer::fixed_width::FixedWidthSlicer;
use evolution_slicer::slicer::Slicer;
use evolution_writer::writer::WriterRef;
use log::info;

use std::path::PathBuf;

use crate::converter::{Converter, ConverterProperties};

/// Struct for converting any fixed-width file to a 
pub struct FixedWidthConverter<'a> {
    slicer: FixedWidthSlicer,
    writer: WriterRef<'a, BuilderRef<Vec<u8>>>,
    builder: BuilderRef<Vec<u8>>,
    schema: FixedWidthSchema,
    properties: ConverterProperties,
}

impl Converter for FixedWidthConverter<'_> {
    ///
    fn convert(&mut self) {
        self.try_convert().unwrap();
    }

    ///
    fn try_convert(&mut self) -> Result<()> {
        if self.properties.multithreaded() {
            self.try_convert_multithreaded().unwrap();
        } else {
            self.try_convert_single_threaded().unwrap();
        }
        Ok(())
    }
}

impl<'a> FixedWidthConverter<'a> {
    /// Create a new instance of a [`FixedWidthConverterBuilder`].
    pub fn builder() -> FixedWidthConverterBuilder<'a> {
        FixedWidthConverterBuilder {
            ..Default::default()
        }
    }

    pub fn try_convert_multithreaded(&mut self) -> Result<()> {
        todo!()
    }

    pub fn try_convert_single_threaded(&mut self) -> Result<()> {
        let mut buffer_capacity: usize = self.properties.read_buffer_size();

        info!("Converting fixed-width file to {} in single-threaded mode.", self.writer.target());
        info!("The file to convert is ~{} in total.", ByteSize::gb((self.slicer.bytes_to_read() / 1_000_000_000) as u64));

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

            let byte_idx_last_line_break: usize = self.slicer.try_find_last_line_break(&buffer)?;
            let n_bytes_left_after_last_line_break: usize =
                buffer_capacity - byte_idx_last_line_break - NUM_BYTES_FOR_NEWLINE;
            
            self.slicer
                .try_seek_relative(-(n_bytes_left_after_last_line_break as i64))?;

            self.builder.try_build_from(buffer)?;
            self.writer.try_write_from(self.builder.try_finish()?)?;

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

#[derive(Default)]
/// Helper struct for building an instance of a [`FixedWidthConverter`].
pub struct FixedWidthConverterBuilder<'a> {
    in_path: Option<PathBuf>,
    schema_path: Option<PathBuf>,
    writer: Option<WriterRef<'a, BuilderRef<Vec<u8>>>>,
    builder: Option<BuilderRef<Vec<u8>>>,
    properties: Option<ConverterProperties>,
}

impl<'a> FixedWidthConverterBuilder<'a> {
    /// Set the relative or absolute path to the fixed-width file that is to be converted.
    pub fn with_in_path(mut self, in_path: PathBuf) -> Self {
        self.in_path = Some(in_path);
        self
    }

    /// Set the relative or absolute path to the schema file that is to be used for the conversion.
    pub fn with_schema_path(mut self, schema_path: PathBuf) -> Self {
        self.schema_path = Some(schema_path);
        self
    }

    /// Set the [`WriterRef`] that will be used to write the converted data to the target format.
    pub fn with_writer(mut self, writer: WriterRef<'a, BuilderRef<Vec<u8>>>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Set the [`BuilderRef`] that will be used to build converted data to the target format.
    pub fn with_builder(mut self, builder: BuilderRef<Vec<u8>>) -> Self {
        self.builder = Some(builder);
        self
    }

    /// Set some optional properties for the conversion.
    pub fn with_properties(mut self, properties: ConverterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Try and create a new instance of a [`FixedWidthConverter`] from the set values.
    /// 
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If any of the required fields have not been set and thus are `None`.
    /// * If the schema deserialization failed.
    /// * If any I/O error occured when trying to open the input and output files.
    /// * If a 
    pub fn try_build(self) -> Result<FixedWidthConverter<'a>> {
        let in_path: PathBuf = self.in_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'in_path' was not provided, exiting...",
            ))
        })?;

        let schema_path: PathBuf = self.schema_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'schema_path' was not provided, exiting...",
            ))
        })?;

        let writer: WriterRef<'a, BuilderRef<Vec<u8>>> = self.writer.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'writer' was not provided, exiting...",
            ))
        })?;

        let builder: BuilderRef<Vec<u8>> = self.builder.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'builder' was not provided, exiting...",
            ))
        })?;

        let properties: ConverterProperties = self.properties.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'properties' was not provided, exiting...",
            ))
        })?;

        let slicer: FixedWidthSlicer = FixedWidthSlicer::try_from_path(in_path)?;
        let schema: FixedWidthSchema = FixedWidthSchema::from_path(schema_path)?;

        Ok(FixedWidthConverter {
            slicer,
            writer,
            builder,
            schema,
            properties,
        })
    }
}