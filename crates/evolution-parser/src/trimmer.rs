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
// File created: 2024-05-31
// Last updated: 2024-05-31
//

use log::warn;
use padder::{Alignment, Symbol};

use std::slice::Iter;

///
pub trait Trimmer {}

///
pub type TrimmerRef = Box<dyn Trimmer>;

///
pub struct TextTrimmer {
    alignment: Alignment,
    symbol: char,
}

impl TextTrimmer {
    ///
    pub fn new(alignment: Alignment, symbol: Symbol) -> Self {
        Self {
            alignment,
            symbol: symbol.into(),
        }
    }

    ///
    pub fn find_byte_indices(&self, bytes: &[u8], n_runes: usize) -> usize {
        let mut utf8_byte_unit: usize = 1;
        let mut n_bytes: usize = 0;
        let mut found_runes: usize = 0;

        let mut iterator: Iter<u8> = bytes.iter(); 

        while found_runes < n_runes {
            let byte: u8 = match iterator.nth(utf8_byte_unit - 1) {
                Some(b) => *b,
                None => break,
            };

            utf8_byte_unit = match byte {
                byte if byte >> 7 == 0 => 1,
                byte if byte >> 5 == 0b110 => 2,
                byte if byte >> 4 == 0b1110 => 3,
                byte if byte >> 3 == 0b11110 => 4,
                _ => panic!("Couldn't parse byte slice, invalid UTF-8 sequence!"),
            };

            found_runes += 1;
            n_bytes += utf8_byte_unit;
        }

        if found_runes != n_runes {
            warn!("Read the entire byte slice but did not find enough runes...");
        }

        n_bytes
    }

    ///
    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.symbol),
            Alignment::Right => text.trim_start_matches::<char>(self.symbol),
            Alignment::Center => text.trim_matches::<char>(self.symbol),
        }
    }
}

