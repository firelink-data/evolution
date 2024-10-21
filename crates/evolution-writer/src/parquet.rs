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
// File created: 2024-05-05
// Last updated: 2024-10-21
//

use evolution_common::error::{Result, SetupError};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use log::warn;
use parquet::arrow::ArrowWriter;

use std::fs::File;
use std::path::PathBuf;

use crate::writer::{Writer, WriterProperties};

/// Struct for writing [`RecordBatch`]s to a parquet file.
pub struct ParquetWriter {
    inner: ArrowWriter<File>,
    properties: WriterProperties,
}

impl ParquetWriter {
    pub fn builder() -> ParquetWriterBuilder {
        ParquetWriterBuilder {
            ..Default::default()
        }
    }

    /// Get the properties for the writer.
    pub fn properties(&self) -> &WriterProperties {
        &self.properties
    }
}

impl<'a> Writer<'a> for ParquetWriter {
    type Buffer = RecordBatch;

    /// Close and finalize the underlying [`ArrowWriter`].
    /// 
    /// # Errors
    /// This function will return an error if an attempt to write was made after calling [`finish`].
    /// 
    /// [`finish`]: ArrowWriter::finish
    fn finish(&mut self) {
        self.try_finish().unwrap();
    }

    /// Get the target path for the output file.
    fn target(&self) -> &str {
        "parquet"
    }

    /// Write the record batch to the output file.
    fn write_from(&mut self, buffer: &mut Self::Buffer) {
        self.try_write_from(buffer).unwrap();
    }

    /// Try and close and finalize the underlying [`ArrowWriter`].
    /// 
    /// # Errors
    /// This function will return an error if an attempt to write was made after calling [`finish`].
    /// 
    /// [`finish`]: ArrowWriter::finish
    fn try_finish(&mut self) -> Result<()> {
        self.inner.finish()?;
        Ok(())
    }

    /// Try and write the record batch to the output file.
    fn try_write_from(&mut self, buffer: &mut Self::Buffer) -> Result<()> {
        self.inner.write(&buffer)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct ParquetWriterBuilder {
    out_path: Option<PathBuf>,
    schema: Option<ArrowSchemaRef>,
    properties: Option<WriterProperties>,
}

impl ParquetWriterBuilder {
    /// Set the relative or absolute path to the output parquet file to create and write to.
    pub fn with_out_path(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    /// Set the schema for the parquet file.
    pub fn with_schema(mut self, schema: ArrowSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the properties for the writer.
    pub fn with_properties(mut self, properties: WriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Create a new instance of a [`ParquetWriter`] from the set values.
    /// 
    /// # Errors
    /// This function might unwrap and panic on an error for the following reasons:
    /// * If the output file path has not been set.
    /// * If the output file could not be opened for writing.
    pub fn build(self) -> ParquetWriter {
        self.try_build().unwrap()
    }

    /// Try and create a new instance of a [`ParquetWriter`] from the set values.
    /// 
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If the output file path has not been set.
    /// * If the output file could not be opened for writing.
    pub fn try_build(self) -> Result<ParquetWriter> {
        let out_path: PathBuf = self.out_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'out_path' was not provided, exiting...",
            ))
        })?;

        let schema: ArrowSchemaRef = self.schema.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'schema' was not provided, exiting...",
            ))
        })?;

        let properties: WriterProperties = match self.properties {
            Some(p) => p,
            None => {
                warn!("No properties were set for the output writer, using default values...");
                WriterProperties::default()
            }
        };

        let inner: ArrowWriter<File> = ArrowWriter::try_new(File::create(&out_path)?, schema, Some(properties.clone().into()))?;

        Ok(ParquetWriter { inner, properties })
    }
}