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
// File created: 2024-05-07
// Last updated: 2024-05-31
//

use arrow::array::BooleanBuilder as BooleanArray;
#[cfg(debug_assertions)]
use log::debug;
use evolution_common::error::{ExecutionError, Result};
use evolution_parser::datatype::BooleanParser;
use log::warn;

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
    pub fn new(
        name: String,
        n_runes: usize,
        is_nullable: bool,
        parser: BooleanParser,
    ) -> Self {
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
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize> {
        let n_bytes_in_column: usize = match self.parser.try_parse(&bytes, self.n_runes) {
            (n, Some(v)) => {
                self.inner.append_value(v);
                n
            },
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
            },
        };

        Ok(n_bytes_in_column)
    }
}
