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
* Last updated: 2024-05-08
*/

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use std::default::Default;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

///
pub(crate) trait Writer: Debug {
    fn write(&mut self, buffer: &Vec<u8>);
    fn finish(&mut self);
}

///
#[derive(Debug)]
pub(crate) struct FixedLengthFileWriter {
    inner: File,
}

///
impl FixedLengthFileWriter {
    pub fn new(file: &PathBuf) -> Self {
        let out_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
            .expect("Could not open target output file!");

        Self { inner: out_file }
    }
}

///
impl Writer for FixedLengthFileWriter {
    fn write(&mut self, buffer: &Vec<u8>) {
        self.inner
            .write_all(buffer)
            .expect("Bad buffer, output write failed!");
    }

    fn finish(&mut self) {
        todo!();
    }
}

///
#[derive(Debug)]
pub(crate) struct ParquetWriter {
    out_file: PathBuf,
    inner: Option<ArrowWriter<File>>,
    record_batch: Option<RecordBatch>,
    properties: Option<WriterProperties>,
}

///
impl ParquetWriter {
    /// Create a new [`ParquetWriter`] to target the provided file as output.
    ///
    /// # Panics
    /// Iff the specified target file could not opened.
    pub fn new(file: &PathBuf) -> Self {
        Self {
            out_file: file.to_owned(),
            inner: None,
            record_batch: None,
            properties: None,
        }
    }

    ///
    pub fn set_record_batch(&mut self, record_batch: RecordBatch) {
        self.record_batch = Some(record_batch);
    }

    ///
    pub fn set_properties(&mut self, properties: WriterProperties) {
        self.properties = Some(properties);
    }

    ///
    pub fn setup_arrow_writer(&mut self) {
        if !self.record_batch.is_some() {
            panic!("");
        }

        if !self.properties.is_some() {
            panic!("");
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.out_file)
            .expect("Could not open target output file!");

        let writer: ArrowWriter<File> = ArrowWriter::try_new(
            file,
            self.record_batch.clone().unwrap().schema(),
            self.properties.clone(),
        )
        .expect("Could not create `ArrowWriter` from provided `RecordBatch`!");

        self.inner = Some(writer);
    }

    ///
    pub fn write_batch(&mut self, batch: &RecordBatch) {
        match self.inner {
            Some(ref mut writer) => {
                writer
                    .write(batch)
                    .expect("Could not write RecordBatch with ArrowWriter!");
            }
            None => {
                panic!("The `ParquetWriter` has not been set up properly, missing `ArrowWriter`!")
            }
        };
    }
}

impl Writer for ParquetWriter {
    fn write(&mut self, _buffer: &Vec<u8>) {
        todo!();
    }

    /// Close and finalize the underlying Parquet writer buffer.
    ///
    /// # Panics
    /// If either the [`ParquetWriter`] has not been setup with an [`ArrowWriter`] or
    /// if the writer tries to write something efter calling finish (race condition).
    fn finish(&mut self) {
        match self.inner {
            Some(ref mut writer) => {
                writer.finish().expect("");
            }
            None => {
                panic!("The `ParquetWriter` has not been set up properly, missing `ArrowWriter`!")
            }
        };
    }
}

/// Find and create a matching Writer for the provided file type.
///
/// # Panics
/// This function can panic for three different reasons:
///  - If the the provided file name does not contain an extension (characters after the .).
///  - If the the file name is not valid unicode.
///  - If the user provided an extension which is not yet supported by the program.
pub(crate) fn writer_from_file_extension(file: &PathBuf) -> Box<dyn Writer> {
    match file
        .extension()
        .expect("No file extension in file name!")
        .to_str()
        .unwrap()
    {
        "flf" => Box::new(FixedLengthFileWriter::new(file)),
        "parquet" => Box::new(ParquetWriter::new(file)),
        _ => panic!(
            "Could not find a matching writer for file extension: {:?}",
            file.extension().unwrap(),
        ),
    }
}

#[cfg(test)]
mod tests_writer {
    use std::fs;

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

        let _flfw = FixedLengthFileWriter::new(&path);
        let _expected = OpenOptions::new().append(true).open(&path).unwrap();

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
