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
use crate::datatype::DataType;
use crate::parser;

pub(crate) trait ColumnTrimmer {
    fn find_start_stop(&self,data: &[u8], runes: i16) -> (usize, usize);
    // TODO @Willhelm perhaps add trim function here ?
}

pub(crate) fn count_rune_bytelength (data: &[u8], runes: i16) -> usize {
        let mut eat = data.iter();
        let mut counted_runes = 0;
        let mut len: usize = 0;
        let mut units = 1;

        while counted_runes < runes as usize {
            let byten = eat.nth(units - 1);

            let bb: u8 = match byten {
                None => {
                    return len;
                }
                Some(b) => *b,
            };

            units = match bb {
                bb if bb >> 7 == 0 => 1,
                bb if bb >> 5 == 0b110 => 2,
                bb if bb >> 4 == 0b1110 => 3,
                bb if bb >> 3 == 0b11110 => 4,
                _bb => {
                    // TODO BAD ERROR HANDL
                    panic!("Incorrect UTF-8 sequence");
                    #[allow(unreachable_code)]
                    0
                }
            };

            len += units;
            counted_runes += 1;
        }

        len
    }

pub(crate) struct UtfNotAligned {

}
impl ColumnTrimmer for UtfNotAligned {
    fn find_start_stop(&self, data: &[u8], runes: i16) -> (usize, usize) {
        (0,count_rune_bytelength(data,runes)-1)
    }
}

pub(crate) struct FloatLeftAligned {
    trim_symbol: Symbol
}
impl ColumnTrimmer for crate::trimmer::FloatLeftAligned {
    fn find_start_stop(&self, data: &[u8], runes: i16) -> (usize, usize) {
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

            if let 48..=57 | 43..=46  = bb {
                start += 1;
                counted_runes += 1;
                break
            }

            return (start, stop);

        }

        (start, stop)
    }
}

pub(crate) struct FloatRightAligned {
    trim_symbol: Symbol
}
impl ColumnTrimmer for FloatRightAligned {
    fn find_start_stop(&self, data: &[u8], runes: i16) -> (usize, usize) {
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

            if let 48..=57 | 43..=46  = bb {
                return (start, stop);
            }

            start += 1;
            counted_runes += 1;
        }

        (start, stop)

    }
}
pub(crate) struct  CharLeftAligned {
    trim_symbol: Symbol
}
impl ColumnTrimmer for crate::trimmer::CharLeftAligned {
    fn find_start_stop(&self, data: &[u8], runes: i16) -> (usize, usize) {
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

            match bb {
                101..=132 | 141..=172 => {start += 1;counted_runes += 1;}
                _ => {}
            };
            return (start, stop);
        }

        (start, stop)
    }
}

pub(crate) struct  CharRightAligned {
    trim_symbol: Symbol
}
impl ColumnTrimmer for CharRightAligned {
    fn find_start_stop(&self, data: &[u8], runes: i16) -> (usize, usize) {
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

            match bb {
                101..=132 | 141..=172 => return (start, stop),
                _ => {}
            };
            start += 1;
            counted_runes += 1;
        }

        (start, stop)
    }
}


pub(crate) struct NumLeftAligned {
    trim_symbol: Symbol
}
impl ColumnTrimmer for crate::trimmer::NumLeftAligned {
    fn find_start_stop(&self, data: &[u8], runes: i16) -> (usize, usize) {
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
                { start += 1;counted_runes += 1; };
            }
             (start, stop);
        }

        (start, stop)

    }
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


pub(crate) fn trimmer_factory(dtype: DataType,alignment: Alignment, trim_symbol: Symbol) -> Box<dyn ColumnTrimmer> {
    match (dtype,alignment) {
        (DataType::Boolean,padder::Alignment::Left) => {Box::new(crate::trimmer::CharLeftAligned { trim_symbol  })}
        (DataType::Float16,padder::Alignment::Left) => {Box::new(crate::trimmer::FloatLeftAligned { trim_symbol  })}
        (DataType::Float32,padder::Alignment::Left) => {Box::new(crate::trimmer::FloatLeftAligned { trim_symbol  })}
        (DataType::Float64,padder::Alignment::Left) => {Box::new(crate::trimmer::FloatLeftAligned { trim_symbol  })}
        (DataType::Int16,padder::Alignment::Left) => {Box::new(crate::trimmer::NumLeftAligned { trim_symbol  })}
        (DataType::Int32,padder::Alignment::Left) => {Box::new(crate::trimmer::NumLeftAligned { trim_symbol  })}
        (DataType::Int64,padder::Alignment::Left) => {Box::new(crate::trimmer::NumLeftAligned { trim_symbol  })}
        (DataType::Boolean,padder::Alignment::Right) => {Box::new(crate::trimmer::CharRightAligned { trim_symbol  })}
        (DataType::Float16,padder::Alignment::Right) => {Box::new(crate::trimmer::FloatRightAligned { trim_symbol  })}
        (DataType::Float32,padder::Alignment::Right) => {Box::new(crate::trimmer::FloatRightAligned { trim_symbol  })}
        (DataType::Float64,padder::Alignment::Right) => {Box::new(crate::trimmer::FloatRightAligned { trim_symbol  })}
        (DataType::Int16,padder::Alignment::Right) => {Box::new(crate::trimmer::NumRightAligned { trim_symbol  })}
        (DataType::Int32,padder::Alignment::Right) => {Box::new(crate::trimmer::NumRightAligned { trim_symbol  })}
        (DataType::Int64,padder::Alignment::Right) => {Box::new(crate::trimmer::NumRightAligned { trim_symbol  })}
        (DataType::Boolean,padder::Alignment::Center) => {Box::new(crate::trimmer::CharLeftAligned { trim_symbol  })}
        (DataType::Float16,padder::Alignment::Center) => {Box::new(crate::trimmer::FloatLeftAligned { trim_symbol  })}
        (DataType::Float32,padder::Alignment::Center) => {Box::new(crate::trimmer::FloatLeftAligned { trim_symbol  })}
        (DataType::Float64,padder::Alignment::Center) => {Box::new(crate::trimmer::FloatLeftAligned { trim_symbol  })}
        (DataType::Int16,padder::Alignment::Center) => {Box::new(crate::trimmer::NumLeftAligned { trim_symbol  })}
        (DataType::Int32,padder::Alignment::Center) => {Box::new(crate::trimmer::NumLeftAligned { trim_symbol  })}
        (DataType::Int64,padder::Alignment::Center) => {Box::new(crate::trimmer::NumLeftAligned { trim_symbol  })}

        (DataType::Utf8,_) => {Box::new(crate::trimmer::UtfNotAligned {   })}
        (DataType::LargeUtf8,_) => {Box::new(crate::trimmer::UtfNotAligned {   })}


    }


}
