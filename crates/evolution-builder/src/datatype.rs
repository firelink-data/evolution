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
// File created: 2024-05-07
// Last updated: 2024-10-11
//

use arrow::array::{
    ArrayRef, BooleanBuilder as BooleanArray, Float16Builder as Float16Array,
    Float32Builder as Float32Array, Float64Builder as Float64Array, Int16Builder as Int16Array,
    Int32Builder as Int32Array, Int64Builder as Int64Array, StringBuilder as Utf8Array,
};
use evolution_common::error::{ExecutionError, Result};
use evolution_parser::datatype::{BooleanParser, FloatParser, IntParser, Utf8Parser};
use half::f16;
use log::warn;

use std::sync::Arc;

use crate::builder::ColumnBuilder;

///
pub struct BooleanColumnBuilder {
    inner: BooleanArray,
    parser: BooleanParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl BooleanColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: BooleanParser) -> Self {
        Self {
            inner: BooleanArray::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for BooleanColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Boolean' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Boolean' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Float16ColumnBuilder {
    inner: Float16Array,
    parser: FloatParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Float16ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: FloatParser) -> Self {
        Self {
            inner: Float16Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Float16ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse::<f16>(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Float16' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Float16' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Float32ColumnBuilder {
    inner: Float32Array,
    parser: FloatParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Float32ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: FloatParser) -> Self {
        Self {
            inner: Float32Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Float32ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse::<f32>(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Float32' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Float32' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Float64ColumnBuilder {
    inner: Float64Array,
    parser: FloatParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Float64ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: FloatParser) -> Self {
        Self {
            inner: Float64Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Float64ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse::<f64>(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Float64' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Float64' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Int16ColumnBuilder {
    inner: Int16Array,
    parser: IntParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Int16ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: IntParser) -> Self {
        Self {
            inner: Int16Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Int16ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse::<i16>(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Int16' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Int16' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Int32ColumnBuilder {
    inner: Int32Array,
    parser: IntParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Int32ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: IntParser) -> Self {
        Self {
            inner: Int32Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Int32ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse::<i32>(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Int32' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Int32' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Int64ColumnBuilder {
    inner: Int64Array,
    parser: IntParser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Int64ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: IntParser) -> Self {
        Self {
            inner: Int64Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Int64ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse::<i64>(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Int64' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Int64' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
pub struct Utf8ColumnBuilder {
    inner: Utf8Array,
    parser: Utf8Parser,
    name: String,
    n_runes: usize,
    is_nullable: bool,
}

impl Utf8ColumnBuilder {
    ///
    pub fn new(name: String, n_runes: usize, is_nullable: bool, parser: Utf8Parser) -> Self {
        Self {
            inner: Utf8Array::new(),
            parser,
            name,
            n_runes,
            is_nullable,
        }
    }
}

impl ColumnBuilder for Utf8ColumnBuilder {
    ///
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse(bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            }
            (n, None) => {
                if self.is_nullable {
                    warn!("Could not parse byte slice to 'Int64' datatype, appending null.");
                    self.inner.append_null();
                    n
                } else {
                    return Err(Box::new(ExecutionError::new(
                        "Could not parse byte slice to 'Int64' datatype, column is not nullable, exiting...",
                    )));
                }
            }
        };

        Ok(n_bytes_in_column)
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}
