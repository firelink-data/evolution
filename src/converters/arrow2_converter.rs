use std::fs;
use std::fs::File;
use std::str;
use std::path::PathBuf;
use str::from_utf8_unchecked;
use arrow2::array::Array;
use rayon::prelude::*;
use crate::converters::arrow2_builder::{ColumnBuilder, MasterBuilder};
use std::sync::Arc;
use crate::converters::{Converter};
use crate::slicers::{find_last_nl, FnLineBreak, Slicer};
use crate::slicers::old_slicer::{old_slicer};

pub(crate) struct Slice2Arrow2<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak,
    pub(crate) master_builder: MasterBuilder<'a>
}

impl Converter for Slice2Arrow2<'_> {
    fn set_line_break_handler(&mut self, fnl: FnLineBreak) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnLineBreak {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&[u8]>) -> usize {
        let mut bytes_processed: usize = 0;
//        let chunks:Chunk<?>;
        let arc_masterbuilder = Arc::new(&self.master_builder);
        // TODO declare a array of chunks[slices.len]  , pass it on to the parse_slice funktion
        slices.par_iter().enumerate().for_each(|(i, n)| {
            let arc_mastbuilder_clone = Arc::clone(&arc_masterbuilder);
            parse_slice(i, n, &arc_mastbuilder_clone);
        });

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
}


