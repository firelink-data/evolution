use std::fs;
use std::fs::File;
use std::str;
use std::io::Write;
use std::path::PathBuf;
use str::from_utf8_unchecked;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use rayon::prelude::*;
use crate::builder::{ColumnBuilder, MasterBuilder};
use crate::{builder, slicer};
use crate::slicer::{find_last_nl, FnLineBreak, SampleSliceAggregator, SlicerProcessor};
use substring::Substring;

pub(crate) struct Slice2Arrowchunk<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak,
    pub(crate) master_builder: MasterBuilder<'a>
}

impl SlicerProcessor for Slice2Arrowchunk<'_> {
    fn set_line_break_handler(&mut self, fnl: FnLineBreak) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnLineBreak {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&[u8]>) -> usize {
        let mut bytes_processed: usize = 0;
//        let chunks:Chunk<?>;

        // TODO declare a array of chunks[slices.len]  , pass it on to the parse_slice funktion
        slices.par_iter().enumerate().for_each(|(i, n)| parse_slice(i, n, &self.master_builder));

        bytes_processed
    }
}
fn parse_slice(i:usize, n: &&[u8],master_builder: &MasterBuilder)  {


    println!("index {} {}", i, n.len());
//    let builders: Vec<Box<dyn ColumnBuilder>>;
    let start_byte_pos=0;

    // TODO make safe/unsafe configurable
    let text:&str = unsafe {
        from_utf8_unchecked(&n)
    };

    let offset=0;
//    let builders: Vec<Box<dyn ColumnBuilder>> =master_builder
 //   master_builder

//    for mut builder in builders {
//            let amount_of_chars=builder.lenght_in_chars() as usize;
//            builder.parse_value(text.substring(offset,offset+amount_of_chars));
//    }
    // parse each line
    // when all lines are parsed , create an Chunk acording to https://docs.rs/arrow2/latest/arrow2/
}

pub(crate) fn parse_from_schema(
    schema_path: PathBuf,
    _in_file_path: PathBuf,
    _out_file_path: PathBuf,
    _n_threads: i16,
) {
//    let mut builders: Vec<Box<dyn ColumnBuilder>> = Vec::new();

    // TODO allow custom datatypes to be added/provided to builders.

   let masterBuilder = MasterBuilder::builder_factory(schema_path);
// &mut builders
    let in_file = fs::File::open(&_in_file_path).expect("bbb");

    let out_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(_out_file_path)
        .expect("aaa");


    let s2a: Box<Slice2Arrowchunk> = Box::new(Slice2Arrowchunk { file_out: out_file, fn_line_break: find_last_nl, master_builder: masterBuilder });
    slicer::slice_and_process(s2a, in_file, _n_threads as usize);

}
