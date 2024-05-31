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
// File created: 2024-05-08
// Last updated: 2024-05-31
//

use padder::{Alignment, Symbol};

use std::str::from_utf8_unchecked;
use std::usize;

use crate::parser::Parser;
use crate::trimmer::TextTrimmer;

///
pub struct BooleanParser {
    trimmer: TextTrimmer,
}

impl BooleanParser {
    ///
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self { trimmer: TextTrimmer::new(alignment, trim_symbol) }
    }

    /// Try and parse the byte slice as UTF-8 characters and count the number of
    /// bytes that the boolean was represented as in the byte slice.
    ///
    /// # Safety
    /// This function utilizes the [`from_utf8_unchecked`] function to convert the byte
    /// slice to a string representation. This method is inherently unsafe and might
    /// cause the program to panic. We have to assume that the input bytes are valid
    /// UTF-8, because recovering from the situation where the bytes were not valid UTF-8
    /// is not possible since then we don't know how far into the buffer we need to read.
    ///
    /// # Performance
    /// The function [`from_utf8_unchecked`] will put the string slice on the stack and not
    /// perform any heap allocations. As such, we need to know the lifetimes of it.
    pub fn try_parse(&self, bytes: &[u8], n_runes: usize) -> (usize, Option<bool>) {
        let end_byte_idx: usize
            = self.trimmer.find_byte_indices(bytes, n_runes);
        let text: &str = unsafe {
            from_utf8_unchecked(&bytes[..end_byte_idx])
        };

        (
            end_byte_idx,
            self.trimmer.trim(text).parse::<bool>().ok(),
        )
    }
}

impl Parser for BooleanParser {}
