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
// File created: 2023-12-11
// Last updated: 2024-05-13
//


use std::default::Default;
use std::slice::Iter;
#[cfg(feature = "nightly")]
use core::str::utf8_char_width;

use crate::error::Result;

#[derive(Debug)]
pub(crate) struct Slicer {
    n_threads: usize,
    multithreading: bool,
}

///
impl Slicer {
    ///
    pub fn builder() -> SlicerBuilder {
        SlicerBuilder {
            ..Default::default()
        }
    }

    /// Calculate how many bytes correspond to how many rune in the provided byte slice.
    /// See this link https://en.wikipedia.org/wiki/UTF-8 for details on how this works.
    ///
    /// # Panics
    /// Iff the byte slice is not a valid utf-8 sequence.
    #[cfg(not(feature = "nightly"))]
    pub fn find_num_bytes_for_num_runes(&self, bytes: &[u8], num_runes: usize) -> usize {
        let mut found_runes: usize = 0;
        let mut num_bytes: usize = 0;
        let mut byte_units: usize = 1;

        let mut iterator: Iter<u8> = bytes.iter();

        while found_runes < num_runes {
            let byte = match iterator.nth(byte_units - 1) {
                Some(b) => *b,
                None => break,
            };

            byte_units = match byte {
                byte if byte >> 7 == 0 => 1,
                byte if byte >> 5 == 0b110 => 2,
                byte if byte >> 4 == 0b1110 => 3,
                byte if byte >> 3 == 0b11110 => 4,
                _ => panic!("Invalid utf-8 sequence!"),
            };

            found_runes += 1;
            num_bytes += byte_units;
        }

        num_bytes
    }

    /// Calculate how many bytes correspond to how many runes in the provided byte slice.
    #[cfg(feature = "nightly")]
    pub fn find_num_bytes_for_num_runes(&self, bytes: &[u8], num_runes: usize) -> usize {
        let mut found_runes: usize = 0;
        let mut num_bytes: usize = 0;
        let mut byte_units: usize = 1;

        let mut iterator: Iter<u8> = bytes.iter();

        while found_runes < num_runes {
            let byte = match iterator.nth(byte_units - 1) {
                Some(b) => *b,
                None => break,
            };

            byte_units = utf8_char_width(byte);

            found_runes += 1;
            num_bytes += byte_units;
        }

        num_bytes
    }

    /// Find all line breaks in a slice of bytes representing utf-8 encoded data.
    /// This method looks for a line-feed (LF) character, represented as `\n`.
    /// The hexadecimal code for `\n` is 0x0a.
    ///
    /// # Panics
    /// Iff the byte slice is empty.
    #[cfg(not(target_os = "windows"))]
    pub fn find_line_breaks(&self, bytes: &[u8], buffer: &mut Vec<usize>) {
        if bytes.is_empty() {
            panic!("Byte slice was empty!");
        }

        (0..bytes.len()).for_each(|idx| {
            if bytes[idx] == 0x0a {
                buffer.push(idx);
            }
        });
    }

    /// Find all line breaks in a slice of bytes representing utf-8 encoded data.
    /// This method looks for windows OS specific line break characters called
    /// carriage-return (CR) and line-feed (LF). They are represented as the
    /// `\r` and `\n` utf-8 characters. The hexadecimal code for `\r` is 0x0d
    /// and `\n` is 0x0a.
    ///
    /// # Panics
    /// Iff the byte slice is empty.
    #[cfg(target_os = "windows")]
    pub fn find_line_breaks(&self, bytes: &[u8], buffer: &mut Vec<usize>) {
        if bytes.is_empty() {
            panic!("Byte slice was empty!");
        }

        (1..bytes.len()).for_each(|idx| {
            if (bytes[idx - 1] == 0x0d) && (bytes[idx] == 0x0a) {
                buffer.push(idx - 1);
            }
        });
    }
}

///
#[derive(Debug, Default)]
pub(crate) struct SlicerBuilder {
    n_threads: Option<usize>,
}

///
impl SlicerBuilder {
    ///
    pub fn num_threads(mut self, n_threads: usize) -> Self {
        self.n_threads = Some(n_threads);
        self
    }

    ///
    pub fn build(self) -> Result<Slicer> {
        let n_threads = match self.n_threads {
            Some(n) => n,
            None => 1,
        };

        let multithreading = n_threads > 1;

        Ok(Slicer {
            n_threads,
            multithreading,
        })
    }
}
