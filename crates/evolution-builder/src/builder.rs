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

use arrow::array::ArrayRef;
use evolution_common::error::Result;
use evolution_common::NUM_BYTES_FOR_NEWLINE;

///
pub trait Builder: From<Vec<ColumnBuilderRef>> {}

///
pub type BuilderRef = Box<dyn Builder>;

///
pub trait ColumnBuilder: Send + Sync {
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize>;
    fn finish(&mut self) -> (&str, ArrayRef);
}

///
pub type ColumnBuilderRef = Box<dyn ColumnBuilder>;

///
pub struct ParquetBuilder {
    columns: Vec<ColumnBuilderRef>,
}

impl ParquetBuilder {
    ///
    pub fn try_build_from_slice(&mut self, buffer: &[u8]) -> Result<()> {
        let mut idx: usize = 0;
        while idx < buffer.len() {
            for column in self.columns.iter_mut() {
                idx += column.try_build_column(&buffer[idx..])?;
            }
            idx += NUM_BYTES_FOR_NEWLINE;
        }

        Ok(())
    }

    ///
    pub fn columns(&mut self) -> &mut Vec<ColumnBuilderRef> {
        &mut self.columns
    }
}

impl From<Vec<ColumnBuilderRef>> for ParquetBuilder {
    fn from(columns: Vec<ColumnBuilderRef>) -> Self {
        Self { columns }
    }
}

impl Builder for ParquetBuilder {}
