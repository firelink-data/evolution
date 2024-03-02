
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::ipc::write::Record;

use crate::schema;

use std::fs::File;
use std::str;
use std::path::PathBuf;
use str::from_utf8_unchecked;
use arrow2::array::{Array, MutablePrimitiveArray};
use rayon::prelude::*;
use std::sync::Arc;
use arrow2::types::NativeType;
use crate::converters::{Converter};
use crate::slicers::{find_last_nl, FnLineBreak, Slicer};


pub(crate) struct Slice2Arrow2<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak<'a>,
    pub(crate) master_builder: MasterBuilder<'a>
}

impl<'a> Converter<'a> for Slice2Arrow2<'a> {
    fn set_line_break_handler(&'a mut self, fnl: FnLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&'a self) -> FnLineBreak<'a> {
        self.fn_line_break
    }

    fn process(& mut self, slices: Vec<& [u8]>) -> usize {
        let mut bytes_processed: usize = 0;
//        let chunks:Chunk<?>;
        let arc_masterbuilder = Arc::new(& self.master_builder);
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

pub(crate) struct ColumnBuilderType<T1: NativeType> {
    pub rows: MutablePrimitiveArray<T1>,
}

impl ColumnBuilder for ColumnBuilderType<i32> {
    fn parse_value(&mut self, name: &str)
        where
            Self: Sized,
    {
        match name.parse::<i32>() {
            Ok(n) => {
                self.rows.push(Some(n));
                n
            }
            Err(_e) => {
                self.nullify();
                0
            }
        };
    }

    fn finish_column(&mut self)
        where
            Self: Sized,
    {
        todo!()
    }

    fn nullify(&mut self)
        where
            Self: Sized,
    {
        self.rows.push(None);
    }

    fn lenght_in_chars(&mut self) -> i16 {
        todo!()
    }
}

impl ColumnBuilder for ColumnBuilderType<i64> {
    fn parse_value(&mut self, name: &str)
        where
            Self: Sized,
    {
        match name.parse::<i64>() {
            Ok(n) => {
                self.rows.push(Some(n));
                n
            }
            Err(_e) => {
                self.nullify();
                0
            }
        };
    }

    fn finish_column(&mut self)
        where
            Self: Sized,
    {
        todo!()
    }

    fn nullify(&mut self)
        where
            Self: Sized,
    {
        self.rows.push(None);
    }

    fn lenght_in_chars(&mut self) -> i16 {
        todo!()
    }
}



///
#[allow(dead_code)]
struct FixedField {
    /// The destination field.
    field: Field,

    /// The source datatype.
    dtype: DataType,

    ///
    len: u32,

    ///
    id: u32,
}

///
#[allow(dead_code)]
struct FixedRow<'a> {
    ///
    fixed_fields: Vec<&'a FixedField>,
}

///
#[allow(dead_code)]
struct FixedTableChunk<'a> {
    ///
    chunk_idx: u32,

    ///
    fixed_table: &'a FixedTable<'a>,

    column_builders: Vec<Box<dyn ColumnBuilder>>,

    // record_builder: Vec<Box< ??? >>
    records: Vec<&'a Record<'a>>,

    bytes: Vec<u8>,
}

///
#[allow(dead_code)]
struct FixedTable<'a> {
    ///
    bytes: Vec<u8>,

    ///
    fixed_table_chunks: Vec<&'a FixedTableChunk<'a>>,

    ///
    row: FixedRow<'a>,

    ///
    schemas: Vec<&'a Schema>,

    ///
    table_n_cols: Vec<u32>,

    ///
    header: Option<String>,

    ///
    footer: Option<String>,

    ///
    encoding: String,
}

///
pub trait ColumnBuilder {
    ///
    fn parse_value(&mut self, name: &str);

    ///
    fn finish_column(&mut self);

    /// I think this function won't be necessary.
    /// `[arrow2]` supports bitmap nulling out-of-the-box.
    fn nullify(&mut self);

    fn lenght_in_chars(&mut self) -> i16;

}

pub(crate) struct MasterBuilder<'a> {
    builders: Vec<Box<dyn Sync + Send + 'a + ColumnBuilder>>
}

unsafe impl Send for MasterBuilder<'_> {}
unsafe impl Sync for MasterBuilder<'_> {}

impl MasterBuilder<'_> {
    pub fn builder_factory<'a>(schema_path: PathBuf) -> Self {
//    builders: &mut Vec<Box<dyn ColumnBuilder>>
        let schema=schema::FixedSchema::from_path(schema_path.into());
        let antal_col=schema.num_columns();

        let mut buildersmut: Vec<Box<dyn ColumnBuilder + Send + Sync>>=Vec::with_capacity(antal_col);


        for val in schema.iter() {
            match val.dtype().as_str() {
                "i32" => buildersmut.push(Box::new(ColumnBuilderType::<i32> {
                    rows: MutablePrimitiveArray::new(),
                })),
                "i64" => buildersmut.push(Box::new(ColumnBuilderType::<i64> {
                    rows: MutablePrimitiveArray::new(),
                })),

                &_ => {}
            };
        }

        let builders = buildersmut;
        MasterBuilder { builders }

    }

}