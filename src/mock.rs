/*
* MIT License
*
* Copyright (c) 2023 Firelink Data
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
* File created: 2023-11-28
* Last updated: 2023-12-14
*/

use crate::schema::{self, FixedSchema};
use log::{debug, info};
use padder;
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

pub(crate) static DEFAULT_MOCKED_FILENAME_LEN: usize = 16;
pub(crate) static DEFAULT_ROW_BUFFER_LEN: usize = 1024 * 1024;

///
pub struct FixedMocker {
    schema: schema::FixedSchema,
}

///
impl FixedMocker {
    ///
    pub fn new(schema: schema::FixedSchema) -> Self {
        Self { schema }
    }

    #[allow(dead_code)]
    pub fn generate_threaded(&self, n_rows: usize, n_threads: usize) {
        let threads: Vec<_> = (0..n_threads)
            .map(|t| {
                thread::spawn(move || {
                    info!("Thread #{} starts working...", t);
                    for _ in 0..n_rows {
                        debug!("hello from #{}", t);
                    }
                    info!("Thread #{} done!", t);
                })
            })
            .collect();

        for handle in threads {
            handle.join().unwrap();
        }
    }

    pub fn generate(&self, n_rows: usize) {
        let rowlen = self.schema.row_len();
        let mut buffer: Vec<u8> =
            Vec::with_capacity(DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2);
        let mut path = PathBuf::from(
            Alphanumeric.sample_string(&mut rand::thread_rng(), DEFAULT_MOCKED_FILENAME_LEN),
        );
        path.set_extension("flf");

        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)
            .expect("Could not open file.");

        let now = SystemTime::now();

        for row in 0..n_rows {
            if row % DEFAULT_ROW_BUFFER_LEN == 0 {
                file.write_all(&buffer).expect("Bad buffer, write failed!");
                buffer = Vec::with_capacity(
                    DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2,
                );
            }

            for col in self.schema.iter() {
                padder::pad_and_push_to_buffer(
                    col.mock().unwrap(),
                    col.length(),
                    padder::Alignment::Right,
                    ' ',
                    &mut buffer,
                );
            }

            buffer.extend_from_slice("\r\n".as_bytes());
        }

        info!(
            "Produced {} rows in {}ms",
            n_rows,
            now.elapsed().unwrap().as_millis(),
        );

        file.write_all(&buffer).expect("Bad buffer, write failed!");
    }
}

pub(crate) fn mock_bool<'a>(len: usize) -> &'a str {
    if len > 3 {
        return "true";
    }
    "false"
}

pub(crate) fn mock_number<'a>(_len: usize) -> &'a str {
    "3"
}

pub(crate) fn mock_string<'a>(_len: usize) -> &'a str {
    "hejj"
}

///
pub(crate) fn mock_from_schema(schema_path: String, n_rows: usize) {
    let schema = schema::FixedSchema::from_path(schema_path.into());
    // let mocker = FixedMocker::new(schema);
    //mocker.generate(n_rows);
    generate_threaded(schema, n_rows, 16);
    //mocker.generate_threaded(n_rows, 5);
}

fn generate_from_thread(thread: usize, schema: Arc<FixedSchema>, n_rows: usize) {
    let rowlen = schema.row_len();
    let mut buffer: Vec<u8> =
        Vec::with_capacity(DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2);
    let mut path = PathBuf::from(
        Alphanumeric.sample_string(&mut rand::thread_rng(), DEFAULT_MOCKED_FILENAME_LEN),
    );
    path.set_extension("flf");

    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(path)
        .unwrap();

    for row in 0..n_rows {
        if row % DEFAULT_ROW_BUFFER_LEN == 0 {
            file.write_all(&buffer).expect("Bad buffer, write failed!");
            buffer =
                Vec::with_capacity(DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2);
        }

        for col in schema.iter() {
            padder::pad_and_push_to_buffer(
                col.mock().unwrap(),
                col.length(),
                padder::Alignment::Right,
                ' ',
                &mut buffer,
            );
        }

        buffer.extend_from_slice("\r\n".as_bytes());
    }

    file.write_all(&buffer).expect("Bad buffer, write failed!");

    println!("thread {} done!", thread);
}

///
fn generate_threaded(schema: FixedSchema, n_rows: usize, n_threads: usize) {
    let n_rows_per_thread = n_rows / n_threads;
    let n_buffers_per_thread = std::cmp::max(n_rows_per_thread / DEFAULT_ROW_BUFFER_LEN, 1);

    let rest = n_rows - (n_rows_per_thread * n_threads);

    println!("n rows per thread: {}", n_rows_per_thread);
    println!("n buffers per thread (total): {}", n_buffers_per_thread);
    println!("slask rest: {}", rest);

    let arc_schema = Arc::new(schema);

    let threads: Vec<_> = (0..n_threads)
        .map(|t| {
            let arc_clone = Arc::clone(&arc_schema);
            thread::spawn(move || generate_from_thread(t, arc_clone, n_rows_per_thread))
        })
        .collect();

    for handle in threads {
        handle.join().unwrap();
    }

    // TODO: generate rest of rows
    println!("still have slask rest: {}", rest);
}

///
pub(crate) fn mock_from_schema_threaded(schema_path: String, n_rows: usize, n_threads: usize) {
    let schema = schema::FixedSchema::from_path(schema_path.into());
    generate_threaded(schema, n_rows, n_threads);
}

#[cfg(test)]
mod tests_mock {
    use super::*;
    use crate::schema::{FixedColumn, FixedSchema};
    use glob::glob;
    use std::fs;

    #[test]
    fn test_mock_from_fixed_schema() {
        let name = String::from("cool Schema");
        let version: i32 = 4198;
        let columns: Vec<FixedColumn> = vec![];

        let schema = FixedSchema::new(name, version, columns);
        let mock = FixedMocker::new(schema);
        mock.generate(10);

        for entry in glob("*.flf*").unwrap() {
            if let Ok(p) = entry {
                let _ = fs::remove_file(p);
            }
        }
    }

    #[test]
    fn test_mock_generate_1000000() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema.json");

        let schema: FixedSchema = FixedSchema::from_path(path);
        let mock = FixedMocker::new(schema);
        mock.generate(1000000);

        for entry in glob("*.flf*").unwrap() {
            if let Ok(p) = entry {
                let _ = fs::remove_file(p);
            }
        }
    }
}
