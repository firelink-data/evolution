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
* File created: 2024-05-05
* Last updated: 2024-05-05
*/

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

///
pub(crate) trait Writer: Debug {
    fn write(&mut self, buffer: &Vec<u8>);
    fn finish(&self);
}

/*
impl Writer for ArrowWriter<File> {
    fn write(&self, buffer: Vec<u8>) {
        todo!();
    }

    fn finish(&self) {
        todo!();
    }
}
*/

///
#[derive(Debug)]
pub(crate) struct FixedLengthFileWriter {
    out_file: File,
}

impl FixedLengthFileWriter {
    pub fn new(out_file: PathBuf) -> Self {
        let out_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&out_file)
            .expect("Could not open target output file!");

        Self { out_file }
    }
}

impl Writer for FixedLengthFileWriter {
    fn write(&mut self, buffer: &Vec<u8>) {
        self.out_file
            .write_all(buffer)
            .expect("Bad buffer, output write failed!");
    }

    fn finish(&self) {
        todo!();
    }
}

pub(crate) fn writer_from_file_extension(output_file: PathBuf) -> Box<dyn Writer> {
    match output_file.extension().expect("No file extension in file name!").to_str().unwrap() {
        "flf" => Box::new(FixedLengthFileWriter::new(output_file)),
        "parquet" => {
            todo!()
        },
        _ => panic!(
            "Could not find a matching writer for file extension: {:?}",
            output_file.extension().unwrap(),
        ),
    }
}
