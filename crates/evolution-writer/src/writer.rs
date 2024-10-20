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
// Last updated: 2024-10-20
//

use evolution_common::error::Result;
use parquet::file::properties::WriterProperties as ArrowWriterProperties;

/// A trait providing functions to write buffered data to some target.
pub trait Writer<'a> {
    type Buffer: 'a;

    fn finish(&mut self);
    fn target(&self) -> &str;
    fn write_from(&mut self, buffer: Self::Buffer);
    
    fn try_finish(&mut self) -> Result<()>;
    fn try_write_from(&mut self, buffer: Self::Buffer) -> Result<()>;
}

/// A short-hand notation for a generic writer implementation.
pub type WriterRef<'a, T> = Box<dyn Writer<'a, Buffer = T>>;

/// Properties for writing converted data to some target.
#[derive(Clone, Debug)]
pub struct WriterProperties {
    pub force_create_new: bool,
    pub create_or_open: bool,
    pub truncate_existing: bool,
}

impl WriterProperties {
    /// Create a new instance of a [`WriterProperties`] with default values.
    /// 
    /// Default values are:
    /// * force_create_new: `false`
    /// * create_or_open: `true`
    /// * truncate_existing: `false`
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Set the option to enforce that there can not already exist a file with the same name.
    pub fn with_force_create_new(mut self, force_create_new: bool) -> Self {
        self.force_create_new = force_create_new;
        self
    }

    /// Set the option to allow there existing a file with the same name in which
    /// case the file will be opened and appended to.
    pub fn with_create_or_open(mut self, create_or_open: bool) -> Self {
        self.create_or_open = create_or_open;
        self
    }

    /// Set the option to truncate the file if it already exists. If the file does
    /// not already exist, then this option has no effect.
    pub fn with_truncate_existing(mut self, truncate_existing: bool) -> Self {
        self.truncate_existing = truncate_existing;
        self
    }
}

impl Default for WriterProperties {
    fn default() -> Self {
        Self {
            force_create_new: false,
            create_or_open: true,
            truncate_existing: false,
        }
    }
}

/// TODO: when we have options in the [`WriterProperties`] that overlap with
/// the [`ArrowWriterProperties`] we need to implement this trait accordingly.
impl From<WriterProperties> for ArrowWriterProperties {
    fn from(_properties: WriterProperties) -> Self {
        Self {
            ..Default::default()
        }
    }
}