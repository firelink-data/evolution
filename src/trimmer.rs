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

use std::cmp::min;
use std::path::PathBuf;
use padder::{Alignment, Symbol};
use crate::parser;

pub(crate) trait ColumnTrimmer {
    fn find_start_stop(&self,data: &[u8], runes: i16) -> (usize, usize);
    // TODO @Willhelm perhaps add trim function here ?
}

pub(crate) struct NumRightAligned {
    trim_symbol: Symbol
}
 impl ColumnTrimmer for NumRightAligned {
    fn find_start_stop(&self,data: &[u8], runes: i16) -> (usize, usize) {
        let mut eat = data.iter();
        let mut counted_runes = 0;
        let mut start: usize = 0;
        let stop: usize = min(data.len(), runes as usize);

        while counted_runes < runes as usize {
            let byten = eat.next();
            let bb: u8 = match byten {
                None => {
                    //TODO  we ran out of data,this is an error, fix later.
                    return (start, stop);
                }
                Some(b) => *b,
            };

            if let 48..=57 = bb {
                return (start, stop);
            }

            start += 1;
            counted_runes += 1;
        }

        (start, stop)
    }
}




pub(crate) fn trimmer_factory(alignment: Alignment, trim_symbol: Symbol) -> Box<dyn ColumnTrimmer> {
     Box::new(crate::trimmer::NumRightAligned { trim_symbol  })

}
