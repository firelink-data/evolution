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
// Last updated: 2024-05-15
//

use half::f16;
use padder::{Alignment, Symbol};

use std::str::from_utf8;

use crate::error::Result;

/// Parse utf8 text as a Boolean datatype.
#[derive(Debug)]
pub(crate) struct BooleanParser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl BooleanParser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<bool> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<bool>()?)
    }

    fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Float16 datatype.
#[derive(Debug)]
pub(crate) struct Float16Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Float16Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<f16> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<f16>()?)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Float32 datatype.
#[derive(Debug)]
pub(crate) struct Float32Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Float32Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<f32> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<f32>()?)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Float64 datatype.
#[derive(Debug)]
pub(crate) struct Float64Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Float64Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<f64> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<f64>()?)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Int16 datatype.
#[derive(Debug)]
pub(crate) struct Int16Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Int16Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<i16> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<i16>()?)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Int32 datatype.
#[derive(Debug)]
pub(crate) struct Int32Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Int32Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<i32> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<i32>()?)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Int64 datatype.
#[derive(Debug)]
pub(crate) struct Int64Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Int64Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<i64> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed.parse::<i64>()?)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a Utf8 datatype.
#[derive(Debug)]
pub(crate) struct Utf8Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl Utf8Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse<'a>(&self, bytes: &'a [u8]) -> Result<&'a str> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}

/// Parse utf8 text as a LargeUtf8 datatype.
#[derive(Debug)]
pub(crate) struct LargeUtf8Parser {
    alignment: Alignment,
    trim_symbol: Symbol,
}

impl LargeUtf8Parser {
    pub fn new(alignment: Alignment, trim_symbol: Symbol) -> Self {
        Self {
            alignment,
            trim_symbol,
        }
    }

    pub fn parse<'a>(&self, bytes: &'a [u8]) -> Result<&'a str> {
        let text: &str = from_utf8(bytes)?;
        let trimmed: &str = self.trim(text);
        Ok(trimmed)
    }

    pub fn trim<'a>(&self, text: &'a str) -> &'a str {
        match self.alignment {
            Alignment::Left => text.trim_end_matches::<char>(self.trim_symbol.into()),
            Alignment::Right => text.trim_start_matches::<char>(self.trim_symbol.into()),
            Alignment::Center => text.trim_matches::<char>(self.trim_symbol.into()),
        }
    }
}


