/*
* MIT License
*
* Copyright (c) 2024 Firelink Data
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
* File created: 2024-05-08
* Last updated: 2024-05-09
*/

use padder::{Alignment, Symbol};

use std::str::from_utf8;

use crate::error::Result;

///
#[derive(Debug)]
pub(crate) struct BooleanParser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

///
impl BooleanParser {
    ///
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    ///
    pub fn parse(&self, bytes: &[u8]) -> Result<bool> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<bool>()?)
    }

    ///
    fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}
