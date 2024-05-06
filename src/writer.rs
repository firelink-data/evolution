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
* Last updated: 2024-05-07
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

///
#[derive(Debug)]
pub(crate) struct FixedLengthFileWriter {
    out_file: File,
}

impl FixedLengthFileWriter {
    pub fn new(file: &PathBuf) -> Self {
        let out_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(file)
            .expect("Could not open target output file!");

        Self { out_file }
    }

    #[allow(dead_code)]
    pub fn out_file(&self) -> &File {
        &self.out_file
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

pub(crate) fn writer_from_file_extension(file: &PathBuf) -> Box<dyn Writer> {
    match file
        .extension()
        .expect("No file extension in file name!")
        .to_str()
        .unwrap()
    {
        "flf" => Box::new(FixedLengthFileWriter::new(file)),
        "parquet" => {
            todo!()
        }
        _ => panic!(
            "Could not find a matching writer for file extension: {:?}",
            file.extension().unwrap(),
        ),
    }
}

#[cfg(test)]
mod tests_writer {
    use std::fs;
    use std::io::IsTerminal;

    use super::*;

    #[test]
    #[should_panic]
    fn test_new_fixed_length_file_writer_panic() {
        let _ = FixedLengthFileWriter::new(&PathBuf::from(""));
    }

    #[test]
    fn test_new_fixed_length_file_writer() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/cool-file.really-cool");

        let flfw = FixedLengthFileWriter::new(&path);
        let expected = OpenOptions::new().append(true).open(&path).unwrap();

        assert_eq!(expected.is_terminal(), flfw.out_file().is_terminal());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_fixed_length_file_writer_write() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/this-file-maybe-exists.xd");

        let mut flfw = FixedLengthFileWriter::new(&path);
        flfw.write(&vec![0u8; 64]);
        fs::remove_file(path).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_fixed_length_file_writer_file_already_exists_panic() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/this-file-will-already-exist.haha");

        let _a = FixedLengthFileWriter::new(&path);
        let _ = match OpenOptions::new().create(true).open(&path) {
            Ok(f) => f,
            Err(_) => {
                fs::remove_file(path).unwrap();
                panic!();
            }
        };
    }
}
