use std::fs::File;
use std::io::Write;
use rayon::prelude::*;
use crate::slicer::{FnLineBreak, SampleSliceAggregator, SlicerProcessor};

pub(crate) struct Slice2Arrowchunk {
// TODO arrow record array or such
// TODO desired minimum records per partion. Save to disk/parquet when passing this value
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak,
}

impl SlicerProcessor for Slice2Arrowchunk {
    fn set_line_break_handler(&mut self, fnl: FnLineBreak) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnLineBreak {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&[u8]>) -> usize {
        let mut bytes_processed: usize = 0;

        slices.par_iter().for_each(|n| print!("slice: {:?}", n));
        for val in slices {
            self.file_out.write_all(val).expect("dasd");

            let l = val.len();
            bytes_processed += l;
        }
        bytes_processed
    }
}
