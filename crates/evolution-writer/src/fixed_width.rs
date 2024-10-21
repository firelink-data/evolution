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
// File created: 2024-10-20
// Last updated: 2024-10-21
//

use evolution_common::error::{Result, SetupError};
use log::warn;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use crate::writer::{Writer, WriterProperties};

/// Struct for writing buffered bytes to a fixed-width file.
pub struct FixedWidthFileWriter {
    inner: File,
    properties: WriterProperties,
}

impl FixedWidthFileWriter {
    /// Create a new instance of a [`FixedWidthFileWriterBuilder`].
    pub fn builder() -> FixedWidthFileWriterBuilder {
        FixedWidthFileWriterBuilder {
            ..Default::default()
        }
    }

    /// Get the properties for the writer.
    pub fn properties(&self) -> &WriterProperties {
        &self.properties
    }
}

impl<'a> Writer<'a> for FixedWidthFileWriter {
    type Buffer = &'a [u8];

    /// Flush any remaining bytes in the output stream, ensuring that all bytes are written to disk.
    /// 
    /// # Panics
    /// If not all bytes could be written due to any I/O error or by reacing EOF.
    fn finish(&mut self) {
        self.try_finish().unwrap();
    }

    /// Get the string representation of the target of the writer.
    fn target(&self) -> &str {
        "fixed-width file"
    }

    /// Write the entire buffer to the file by continuously calling [`write`].
    /// 
    /// # Panics
    /// If and only if any I/O error occured during writing.
    fn write_from(&mut self, buffer: &mut Self::Buffer) {
        self.try_write_from(buffer).unwrap();
    }

    /// Try to flush any remaining bytes in the output stream, ensuring that all bytes are written to disk.
    /// 
    /// # Errors
    /// If not all bytes could be written due to any I/O error or by reacing EOF.
    fn try_finish(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    /// Try to write the entire buffer to the file by continuously calling [`write`].
    /// 
    /// # Errors
    /// If and only if any I/O error occured during writing. Each call to [`write`] might generate
    /// an I/O error, however, no bytes will be written to disk if that actually happens.
    fn try_write_from(&mut self, buffer: &mut Self::Buffer) -> Result<()> {
        self.inner.write_all(buffer)?;
        Ok(())
    }
}

/// A helper struct for building an instance of a [`FixedWidthFileWriter`] struct.
#[derive(Default)]
pub struct FixedWidthFileWriterBuilder {
    out_path: Option<PathBuf>,
    properties: Option<WriterProperties>,
}

impl FixedWidthFileWriterBuilder {
    /// Set the relative or absolute path to the fixed-width file to create and write to.
    pub fn with_out_path(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    /// Set the properties for the writer.
    pub fn with_properties(mut self, properties: WriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Create a new instance of a [`FixedWidthFileWriter`] from the set values.
    /// 
    /// # Panics
    /// This function might unwrap and panic on an error for the following reasons:
    /// * If the output file path has not been set.
    /// * If the output file could not be opened for writing.
    pub fn build(self) -> FixedWidthFileWriter {
        self.try_build().unwrap()
    }

    /// Try to create a new instance of a [`FixedWidthFileWriter`] from the set values.
    /// 
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If the output file path has not been set.
    /// * If the output file could not be opened for writing.
    pub fn try_build(self) -> Result<FixedWidthFileWriter> {
        let out_path: PathBuf = self.out_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'out_path' was not provided, exiting...",
            ))
        })?;

        let properties: WriterProperties = match self.properties {
            Some(p) => p,
            None => {
                warn!("No properties were set for the output writer, using default values...");
                WriterProperties::default()
            }
        };

        let inner: File = OpenOptions::new()
            .write(true)
            .create_new(properties.force_create_new)
            .create(properties.create_or_open)
            .append(properties.create_or_open && !properties.truncate_existing)
            .truncate(properties.truncate_existing && !properties.force_create_new)
            .open(out_path)?;
        
        Ok(FixedWidthFileWriter { inner, properties })
    }
}
