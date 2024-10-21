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
// File created: 2024-10-19
// Last updated: 2024-10-21
//

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

use evolution_common::NUM_BYTES_FOR_NEWLINE;
use evolution_common::error::Result;

use crate::builder::{Builder, ColumnBuilder};

/// A short-hand notation for a parquet specific [`ColumnBuilder`] reference.
pub type ParquetColumnBuilderRef = Box<dyn ColumnBuilder<Output = (String, ArrayRef)>>;

/// A builder for parquet files.
pub struct ParquetBuilder {
    columns: Vec<ParquetColumnBuilderRef>,
}

impl ParquetBuilder {
    /// Borrow the columns of the parquet builder.
    pub fn columns(&mut self) -> &mut Vec<ParquetColumnBuilderRef> {
        &mut self.columns
    }
}

/// Conversion from a vector of [`ParquetColumnBuilderRef`] to a [`ParquetBuilder`].
impl From<Vec<ParquetColumnBuilderRef>> for ParquetBuilder {
    fn from(columns: Vec<ParquetColumnBuilderRef>) -> Self {
        Self { columns }
    }
}

impl Builder for ParquetBuilder {
    type Buffer = Vec<u8>;
    type Output = RecordBatch;

    /// Build the parquet columns from the buffer.
    /// 
    /// # Panics
    /// If any of the [`ColumnBuilder`]s fail to build from the buffer.
    fn build_from(&mut self, buffer: Self::Buffer) {
        self.try_build_from(buffer).unwrap();
    }

    /// Try to build the parquet columns from the buffer.
    /// 
    /// # Errors
    /// If any of the [`ColumnBuilder`]s fail to build from the buffer.
    fn try_build_from(&mut self, buffer: Self::Buffer) -> Result<()> {
        let mut idx: usize = 0;
        while idx < buffer.len() {
            for column in self.columns.iter_mut() {
                idx += column.try_build_column(&buffer[idx..])?;
            }
            idx += NUM_BYTES_FOR_NEWLINE;
        }

        Ok(())
    }

    /// Try to finalize the accumulated columns into a [`RecordBatch`].
    /// 
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * The vec of columns was empty.
    /// * Any column array had a different length.
    fn try_finish(&mut self) -> Result<Self::Output> {
        let mut columns: Vec<(String, ArrayRef)> = Vec::with_capacity(self.columns.len());
        for column in self.columns.iter_mut() {
            columns.push(column.finish());
        }
        let record_batch: RecordBatch = RecordBatch::try_from_iter(columns)?;
        Ok(record_batch)
    }
}