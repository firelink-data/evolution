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
// Last updated: 2024-10-11
//

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use evolution_builder::builder::ParquetBuilder;
use evolution_common::error::{Result, SetupError};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties as ArrowWriterProperties;

use std::fs::{File, OpenOptions};
use std::path::PathBuf;

///
pub struct ParquetWriter {
    inner: ArrowWriter<File>,
    n_columns: usize,
}

impl ParquetWriter {
    ///
    pub fn builder() -> ParquetWriterBuilder {
        ParquetWriterBuilder {
            ..Default::default()
        }
    }

    /// TODO: NOTE THIS ALLOCATES MEMORY ON THE HEAP!!!
    pub fn try_write_from_builder(&mut self, builder: &mut ParquetBuilder) -> Result<()> {
        let mut buffer: Vec<(&str, ArrayRef)> = Vec::with_capacity(self.n_columns);
        for column_builder in builder.columns().iter_mut() {
            buffer.push(column_builder.finish());
        }

        let record_batch: RecordBatch = RecordBatch::try_from_iter(buffer)?;
        self.inner.write(&record_batch)?;

        Ok(())
    }

    /// Close and finalize the underlying arrow writer.
    pub fn try_finish(&mut self) -> Result<()> {
        self.inner.finish()?;
        Ok(())
    }
}

/// A helper struct for building an instance of a [`ParquetWriter`] struct.
#[derive(Default)]
pub struct ParquetWriterBuilder {
    out_path: Option<PathBuf>,
    schema: Option<ArrowSchemaRef>,
    properties: Option<ArrowWriterProperties>,
}

impl ParquetWriterBuilder {
    /// Set the relative or absolute path to the output file to produce.
    pub fn with_out_path(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    /// Set the [`ArrowSchemaRef`] to use for the parquet file.
    pub fn with_arrow_schema(mut self, schema: ArrowSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the [`ArrowWriterProperties`] to use for the [`ArrowWriter`].
    pub fn with_properties(mut self, properties: Option<ArrowWriterProperties>) -> Self {
        self.properties = properties;
        self
    }

    ///
    pub fn try_build(self) -> Result<ParquetWriter> {
        let out_file: File = match self.out_path {
            Some(p) => OpenOptions::new().create(true).append(true).open(p)?,
            None => {
                return Err(Box::new(SetupError::new(
                    "Required field 'out_path' was not provided, exiting...",
                )))
            }
        };

        let schema: ArrowSchemaRef = self.schema.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'schema' was not provided, exiting...",
            ))
        })?;

        let n_columns: usize = schema.all_fields().len();

        // Note, here it is OK for no properties to be set.
        let inner: ArrowWriter<File> = ArrowWriter::try_new(out_file, schema, self.properties)?;

        Ok(ParquetWriter { inner, n_columns })
    }

    ///
    pub fn build(self) -> ParquetWriter {
        self.try_build().unwrap()
    }
}
