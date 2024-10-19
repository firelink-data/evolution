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
// File created: 2024-10-18
// Last updated: 2024-10-18
//

use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use evolution_builder::csv::CsvBuilder;
use evolution_common::error::{Result, SetupError};

pub struct CsvWriter {
    inner: File,
    n_columns: usize,
    properties: CsvWriterProperties,
}

impl CsvWriter {
    pub fn builder() -> CsvWriterBuilder {
        CsvWriterBuilder {
            ..Default::default()
        }
    }

    pub fn try_write_from_builder(&mut self, builder: &mut CsvBuilder) -> Result<()> {
        let mut buffer: Vec<u8> = Vec::with_capacity(self.n_columns);
        for (col_idx, column) in builder.columns().iter_mut().enumerate() {
            buffer.extend(column.finish());
            if col_idx != self.n_columns - 1 {
                buffer.push(self.properties.delimiter() as u8);
            }
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct CsvWriterBuilder {
    out_path: Option<PathBuf>,
    n_columns: Option<usize>,
    properties: Option<CsvWriterProperties>,
}

impl CsvWriterBuilder {
    pub fn with_out_path(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    pub fn with_n_columns(mut self, n_columns: usize) -> Self {
        self.n_columns = Some(n_columns);
        self
    }

    pub fn with_properties(mut self, properties: CsvWriterProperties) -> Self {
        self.properties = Some(properties);
        self
    }

    pub fn try_build(self) -> Result<CsvWriter> {
        let out_file: File = match self.out_path {
            Some(p) => OpenOptions::new().create(true).append(true).open(p)?,
            None => {
                return Err(Box::new(SetupError::new(
                    "Output path not set for CSV writer.",
                )))
            }
        };

        let n_columns: usize = match self.n_columns {
            Some(n) => n,
            None => {
                return Err(Box::new(SetupError::new(
                    "Number of columns not set for CSV writer.",
                )))
            }
        };

        let properties: CsvWriterProperties = match self.properties {
            Some(p) => p,
            None => CsvWriterProperties::default(),
        };

        Ok(CsvWriter {
            inner: out_file,
            n_columns,
            properties,
        })
    }
}

pub struct CsvWriterProperties {
    delimiter: char,
    quote: char,
    escape: char,
}

///
impl CsvWriterProperties {
    /// 
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn with_quote(mut self, quote: char) -> Self {
        self.quote = quote;
        self
    }

    pub fn with_escape(mut self, escape: char) -> Self {
        self.escape = escape;
        self
    }

    pub fn delimiter(&self) -> char {
        self.delimiter
    }

    pub fn quote(&self) -> char {
        self.quote
    }

    pub fn escape(&self) -> char {
        self.escape
    }
}

impl Default for CsvWriterProperties {
    fn default() -> Self {
        CsvWriterProperties {
            delimiter: ',',
            quote: '"',
            escape: '\\',
        }
    }
}