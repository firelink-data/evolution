use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use rayon::prelude::*;
use crate::builder::ColumnBuilder;
use crate::{builder, slicer};
use crate::slicer::{find_last_nl, FnLineBreak, SampleSliceAggregator, SlicerProcessor};

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

        slices.par_iter().enumerate().for_each(|(i, n)| println!("index {} {}", i, n.len()));
        for val in slices {
            self.file_out.write_all(val).expect("dasd");

            let l = val.len();
            bytes_processed += l;
        }
        bytes_processed
    }
}

pub(crate) fn parse_from_schema(
    schema_path: PathBuf,
    _in_file_path: PathBuf,
    _out_file_path: PathBuf,
    _n_threads: i16,
) {
    let mut builders: Vec<Box<dyn ColumnBuilder>> = Vec::new();

    // TODO allow custom datatypes to be added/provided to builders.

    builder::builder_factory(schema_path, &mut builders);

    let in_file = fs::File::open(&_in_file_path).expect("bbb");

    let out_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(_out_file_path)
        .expect("aaa");


    let s2a: Box<Slice2Arrowchunk> = Box::new(Slice2Arrowchunk { file_out: out_file, fn_line_break: find_last_nl });
    slicer::slice_and_process(s2a, in_file, _n_threads as usize);

}
