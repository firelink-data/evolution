//use std::fs;
//use std::fs::File;
use std::path::PathBuf;
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use arrow::array::{ ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use crate::{converters, schema};
use crate::converters::{ColumnBuilder, Converter};
use crate::slicers::{FnFindLastLineBreak, FnLineBreakLen};
use debug_print::{ debug_println};
use parquet::format::FileMetaData;
use tokio::fs::File;
use tokio::task;
use tokio::runtime::Handle;

pub(crate) struct Slice2Arrow<'a> {
//    pub(crate) file_out: File,
//    pub(crate) writer:parquet::arrow::async_writer::AsyncArrowWriter<tokio::fs::File>,
    pub(crate) outfile: &'a PathBuf,
    pub(crate) fn_line_break: FnFindLastLineBreak<'a>,
    pub(crate) fn_line_break_len: FnLineBreakLen,
    pub(crate) masterbuilders:  MasterBuilders,
    pub(crate) writer: Option<parquet::arrow::async_writer::AsyncArrowWriter<tokio::fs::File>>
}

pub(crate) struct MasterBuilders {
      builders:  Vec<Vec<Box<dyn  Sync + Send   + ColumnBuilder>>>,
      schema: arrow_schema::SchemaRef

}

unsafe impl Send for MasterBuilders {}
unsafe impl Sync for MasterBuilders {}

impl MasterBuilders {

    pub fn schema_factory<'a>(& mut self) -> SchemaRef {

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let b: &mut Vec<Box<dyn Sync+Send+ColumnBuilder>>=self.builders.get_mut(0).unwrap();
        let mut br:Vec<(&str, ArrayRef)> = vec![];
        for bb in b.iter_mut() {
            br.push(  bb.finish());
        }

        let batch = RecordBatch::try_from_iter(br).unwrap();
        batch.schema()
    }

    pub fn builders_factory<'a>(schema_path: PathBuf, instances: i16) -> Self {
        let schema = schema::FixedSchema::from_path(schema_path.into());
        let antal_col = schema.num_columns();
        let mut builders:Vec<Vec<Box<dyn ColumnBuilder + Sync + Send>>>=Vec::new();

        for _i in 1..=instances {
            let mut buildersmut:  Vec<Box<dyn ColumnBuilder + Sync + Send>> =  Vec::with_capacity(antal_col);
            for val in schema.iter() {
                match val.dtype().as_str() {
                    "i32" => buildersmut.push(Box::new(HandlerInt32Builder { int32builder: Int32Builder::new(), runes_in_column: val.length(), name: val.name().clone()  }   )),
                    "i64" => buildersmut.push(Box::new(HandlerInt64Builder { int64builder: Int64Builder::new(), runes_in_column: val.length(), name: val.name().clone() }   )),
                    "boolean" => buildersmut.push(Box::new( HandlerBooleanBuilder  { boolean_builder: BooleanBuilder::new(), runes_in_column: val.length(), name: val.name().clone() })),
                    "utf8" => buildersmut.push(Box::new( HandlerStringBuilder {string_builder: StringBuilder::new(), runes_in_column: val.length(), name: val.name().clone() })),

                    &_ => {}
                };
            }
            builders.push(buildersmut);
        }

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let b: &mut Vec<Box<dyn Sync+Send+ColumnBuilder>>=builders.get_mut(0).unwrap();
        let mut br:Vec<(&str, ArrayRef)> = vec![];
        for bb in b.iter_mut() {
            br.push(  bb.finish());
        }

        let batch = RecordBatch::try_from_iter(br).unwrap();
        


        MasterBuilders { builders, schema: batch.schema() }

    }
}


impl<'a> Converter<'a> for Slice2Arrow<'a> {
    fn set_line_break_handler(& mut self, fnl: FnFindLastLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(& self) -> FnFindLastLineBreak<'a> {
        self.fn_line_break
    }
    #[tokio::main]
      async fn process(& mut  self,  slices: Vec<&'a [u8]>) -> usize {
        let bytes_processed: usize = 0;



        let arc_slices = Arc::new(& slices);
        self.masterbuilders.builders.par_iter_mut().enumerate().for_each(|(i,  n)| {

            let arc_slice_clone = Arc::clone(&arc_slices);
            match arc_slice_clone.get(i) {
                None => {}
                Some(_) => {            parse_slice(i, arc_slice_clone.get(i).unwrap(), n,(self.fn_line_break_len)() );}
            }
        });

        self.dump(self.outfile).await;

        bytes_processed

//        let f =self.dump();
//        block_on(f);
//        bytes_processed
    }

    fn finish(&mut self)-> parquet::errors::Result<format::FileMetaData> {
//        self.writer.finish()
        std::result::Result::Ok(FileMetaData {
            version: 0,
            schema: vec![],
            num_rows: 0,
            row_groups: vec![],
            key_value_metadata: None,
            created_by: None,
            column_orders: None,
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        })
    }
}

impl<'a> Slice2Arrow<'a> {
    async fn dump(&mut self, out_file: &PathBuf) {

//            let mut writer: parquet::arrow::async_writer::AsyncArrowWriter<tokio::fs::File> = self.masterbuilders.writer_factory(self.outfile);

            let _out_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(out_file).await.unwrap();

            let props = WriterProperties::builder().set_compression(Compression::SNAPPY)
            .build();

        match self.writer {
            None => {self.writer = Some( parquet::arrow::async_writer::AsyncArrowWriter::try_new(_out_file,self.masterbuilders.schema.clone() , Some(props.clone())).unwrap());}
            Some(_) => {}
        }



        for b in self.masterbuilders.builders.iter_mut() {
                let mut br: Vec<(&str, ArrayRef)> = vec![];

                for bb in b.iter_mut() {
                    br.push(bb.finish());
                }
                let batch = RecordBatch::try_from_iter(br).unwrap();
                debug_println!("num_cols? {:#?}", batch.columns());

                self.writer.as_mut().unwrap().write(&batch).await.expect("Writing batch");
            }
        }
}


fn
parse_slice(i:usize, n: &[u8], builders: &mut Vec<Box<dyn ColumnBuilder +Send + Sync>>, linebreak: usize)  {


    println!("index {} {}", i, n.len());

    let mut cursor:usize = 0;
    while cursor < n.len() {
        for cb in &mut *builders {
            let bytelen = cb.parse_value(&n[cursor..]);
            cursor += bytelen;
        }
        cursor+=linebreak; // TODO adjust to CR/LF mode
    }

}

struct HandlerInt32Builder {
    int32builder: Int32Builder,
    runes_in_column: usize,
    name: String
}

impl ColumnBuilder for HandlerInt32Builder {
    fn parse_value(&mut self, data: &[u8]) ->usize
        where
            Self: Sized,
    {
        let (start,stop)= converters::column_length_num_rightaligned(data, self.runes_in_column as i16);

        match atoi_simd::parse(&data[start..stop]) {
            Ok(n) => {
                self.int32builder.append_value(n);
            }
            Err(_e) => {
                self.int32builder.append_null();
            }
        };
        self.runes_in_column
    }

    fn finish(& mut self) -> (&str,ArrayRef) {
        (self.name.as_str(), Arc::new(self.int32builder.finish()) as ArrayRef)

    }

}
struct HandlerInt64Builder {
    int64builder: Int64Builder,
    runes_in_column: usize,
    name: String
}
impl ColumnBuilder for HandlerInt64Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize
        where
            Self: Sized,
    {
        let (start,stop)= converters::column_length_num_rightaligned(data, self.runes_in_column as i16);
        match atoi_simd::parse(&data[start..stop]) {
            Ok(n) => {
                self.int64builder.append_value(n);
            }
            Err(_e) => {
                self.int64builder.append_null();
            }
        };
        // todo fix below
    self.runes_in_column
    }

    fn finish(& mut self) -> (&str,ArrayRef) {
        (self.name.as_str(), Arc::new(self.int64builder.finish()) as ArrayRef)
    }

}
// Might be better of to copy the actual data to array<str>[colnr]


struct HandlerStringBuilder {
    string_builder: StringBuilder,
    runes_in_column: usize,
    name: String
}
impl ColumnBuilder for HandlerStringBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
        where
            Self: Sized,
    {
        let column_length:usize= converters::column_length(data, self.runes_in_column as i16);
// Me dont like ... what is the cost ? Could it be done once for the whole chunk ?
        let text:&str = unsafe {
            from_utf8_unchecked(&data[..column_length])
        };

        match text.is_empty() {
            false => {
                self.string_builder.append_value(text);
            }
            true => {
                self.string_builder.append_null();
            }
        };
        // todo fix below
    column_length
    }

    fn finish(& mut self) -> (&str,ArrayRef) {
        (self.name.as_str(), Arc::new(self.string_builder.finish()) as ArrayRef)
    }
}

struct HandlerBooleanBuilder {
    boolean_builder: BooleanBuilder,
    runes_in_column: usize,
    name: String
}

impl ColumnBuilder for HandlerBooleanBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
        where
            Self: Sized,
    {
        let (start,stop)= converters::column_length_char_rightaligned(data, self.runes_in_column as i16);

        let text:&str = unsafe {
            from_utf8_unchecked(&data[start..stop])
        };

        match text.parse::<bool>() {
            Ok(n) => {
                self.boolean_builder.append_value(n);
            }
            Err(_e) => {
                self.boolean_builder.append_null();
            }
        };
        // todo fix below
    self.runes_in_column
    }

    fn finish(& mut self) -> (&str,ArrayRef) {
        (self.name.as_str() ,Arc::new(self.boolean_builder.finish()) as ArrayRef)
    }

}


