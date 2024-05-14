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
// File created: 2024-05-05
// Last updated: 2024-05-15
//

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties as ArrowWriterProperties;

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::error::Result;

pub(crate) trait Writer: Debug {
    fn write(&mut self, buffer: &[u8]) -> Result<()>;
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

#[derive(Clone, Debug)]
pub(crate) struct FixedLengthFileWriterProperties {
    create_new: bool,
    create: bool,
    truncate: bool,
}

impl FixedLengthFileWriterProperties {
    pub fn builder() -> FixedLengthFileWriterPropertiesBuilder {
        FixedLengthFileWriterPropertiesBuilder {
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct FixedLengthFileWriterPropertiesBuilder {
    create_new: Option<bool>,
    create: Option<bool>,
    truncate: Option<bool>,
}

impl FixedLengthFileWriterPropertiesBuilder {
    /// Set the option to create a new file, failing if it already exists.
    /// No file is allowed to exist at the target location, also no (dangling)
    /// symlink. In this way, if the call succeeds, the file returned is gauranteed
    /// to be new. This operation is atomic.
    ///
    /// If this option is set, then [`.with_create()`] and [`.with_truncate()`] are ignored.
    pub fn with_create_new(mut self, create_new: bool) -> Self {
        self.create_new = Some(create_new);
        self
    }

    /// Set the option to create a new file, or open it if it already exists.
    /// In order for the file to be created, [`OpenOptions::write`] or
    /// [`OpenOptions::append`] access must also be used.
    pub fn with_create(mut self, create: bool) -> Self {
        self.create = Some(create);
        self
    }

    /// Set the option to truncate a previous file. If a file is sucessfully opened
    /// with this option set it will truncate the file to 0 length if it already exists.
    pub fn with_truncate(mut self, truncate: bool) -> Self {
        self.truncate = Some(truncate);
        self
    }

    pub fn build(self) -> Result<FixedLengthFileWriterProperties> {
        let create_new: bool = self
            .create_new
            .ok_or("Required field 'create_new' is missing or None.")?;

        let create: bool = self
            .create
            .ok_or("Required field 'create' is missing or None.")?;

        let truncate: bool = self
            .truncate
            .ok_or("Required field 'truncate' is missing or None.")?;

        Ok(FixedLengthFileWriterProperties {
            create_new,
            create,
            truncate,
        })
    }
}

#[derive(Debug)]
pub(crate) struct FixedLengthFileWriter {
    inner: File,
}

impl FixedLengthFileWriter {
    pub fn builder() -> FixedLengthFileWriterBuilder {
        FixedLengthFileWriterBuilder {
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct FixedLengthFileWriterBuilder {
    out_file: Option<PathBuf>,
    properties: Option<FixedLengthFileWriterProperties>,
}

impl FixedLengthFileWriterBuilder {
    pub fn with_out_file(mut self, out_file: PathBuf) -> Self {
        self.out_file = Some(out_file);
        self
    }

    pub fn with_properties(mut self, properties: FixedLengthFileWriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    pub fn build(self) -> Result<FixedLengthFileWriter> {
        let out_file: PathBuf = self
            .out_file
            .ok_or("Required field 'out_file' is missing or None.")?;

        let properties: FixedLengthFileWriterProperties = self
            .properties
            .ok_or("Required field 'properties' is missing or None.")?;

        let file: File = OpenOptions::new()
            .write(true)
            .create_new(properties.create_new)
            .create(properties.create)
            .append(properties.create && !properties.truncate)
            .truncate(properties.truncate && !properties.create_new)
            .open(out_file)?;

        Ok(FixedLengthFileWriter { inner: file })
    }
}

impl Writer for FixedLengthFileWriter {
    fn write(&mut self, buffer: &[u8]) -> Result<()> {
        self.inner.write_all(buffer)?;
        Ok(())
    }

    fn write_batch(&mut self, _batch: &RecordBatch) -> Result<()> {
        todo!()
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ParquetWriter {
    inner: ArrowWriter<File>,
}

impl ParquetWriter {
    pub fn builder() -> ParquetWriterBuilder {
        ParquetWriterBuilder {
            ..Default::default()
        }
    }
}

impl Writer for ParquetWriter {
    fn write(&mut self, _buffer: &[u8]) -> Result<()> {
        todo!();
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch)?;
        Ok(())
    }

    /// Close and finalize the underlying Parquet writer buffer.
    ///
    /// # Panics
    /// If either the [`ParquetWriter`] has not been setup with an [`ArrowWriter`] or
    /// if the writer tries to write something efter calling finish (race condition).
    fn finish(&mut self) -> Result<()> {
        self.inner.finish()?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct ParquetWriterBuilder {
    out_file: Option<PathBuf>,
    schema: Option<ArrowSchemaRef>,
    properties: Option<ArrowWriterProperties>,
}

impl ParquetWriterBuilder {
    pub fn with_out_file(mut self, path: PathBuf) -> Self {
        self.out_file = Some(path);
        self
    }

    pub fn with_properties(mut self, properties: ArrowWriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    pub fn with_arrow_schema(mut self, schema: ArrowSchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn build(self) -> Result<ParquetWriter> {
        let out_file: PathBuf = self
            .out_file
            .ok_or("Required field 'out_file' is missing or None.")?;

        let writer_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(out_file)
            .expect("Could not open target output file!");

        let properties: ArrowWriterProperties = self
            .properties
            .ok_or("Required field 'properties' is missing or None.")?;

        let schema: ArrowSchemaRef = self
            .schema
            .ok_or("Required field 'schema' is missing or None.")?;

        let inner: ArrowWriter<File> = ArrowWriter::try_new(writer_file, schema, Some(properties))?;

        Ok(ParquetWriter { inner })
    }
}

#[cfg(test)]
mod tests_writer {
    use std::fs;

    use super::*;

    #[test]
    #[should_panic]
    fn test_new_fixed_length_file_writer_panic() {
        let properties: FixedLengthFileWriterProperties =
            FixedLengthFileWriterProperties::builder()
                .with_create_new(false)
                .with_create(true)
                .with_truncate(false)
                .build()
                .unwrap();

        let _: FixedLengthFileWriter = FixedLengthFileWriter::builder()
            .with_out_file(PathBuf::from(""))
            .with_properties(properties)
            .build()
            .unwrap();
    }

    #[test]
    fn test_new_fixed_length_file_writer() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/cool-file.really-cool");

        let properties: FixedLengthFileWriterProperties =
            FixedLengthFileWriterProperties::builder()
                .with_create_new(true)
                .with_create(false)
                .with_truncate(false)
                .build()
                .unwrap();

        let _: FixedLengthFileWriter = FixedLengthFileWriter::builder()
            .with_out_file(path.clone())
            .with_properties(properties)
            .build()
            .unwrap();

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_fixed_length_file_writer_write() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/this-file-maybe-exists.xd");

        let properties: FixedLengthFileWriterProperties =
            FixedLengthFileWriterProperties::builder()
                .with_create_new(true)
                .with_create(false)
                .with_truncate(false)
                .build()
                .unwrap();

        let mut flfw: FixedLengthFileWriter = FixedLengthFileWriter::builder()
            .with_out_file(path.clone())
            .with_properties(properties)
            .build()
            .unwrap();

        flfw.write(&vec![0u8; 64]).unwrap();
        fs::remove_file(path).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_fixed_length_file_writer_file_already_exists_panic() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/this-file-will-already-exist.haha");

        let properties: FixedLengthFileWriterProperties =
            FixedLengthFileWriterProperties::builder()
                .with_create_new(true)
                .with_create(false)
                .with_truncate(false)
                .build()
                .unwrap();

        let _: FixedLengthFileWriter = FixedLengthFileWriter::builder()
            .with_out_file(path.clone())
            .with_properties(properties.clone())
            .build()
            .unwrap();

        match FixedLengthFileWriter::builder()
            .with_out_file(path.clone())
            .with_properties(properties.clone())
            .build()
        {
            Ok(_) => {}
            Err(_) => {
                fs::remove_file(path).unwrap();
                panic!();
            }
        };
    }
}