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

use std::fs::File;
use std::path::PathBuf;

pub struct CsvWriter {
    inner: File,
    n_columns: usize,
}

impl CsvWriter {
    pub fn builder() -> CsvWriterBuilder {
        CsvWriterBuilder {
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub struct CsvWriterBuilder {
    out_path: Option<PathBuf>,
    properties: CsvWriterProperties,
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