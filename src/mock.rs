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
use crossbeam::channel;
use log::{debug, info};
use rand::distributions::{Alphanumeric, DistString};
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
#[allow(dead_code)]
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

fn generate_from_thread(
    thread: usize,
    schema: Arc<FixedSchema>,
    n_rows: usize,
    sender: channel::Sender<Vec<u8>>,
) {
    let rowlen = schema.row_len();
    let mut buffer: Vec<u8> =
        Vec::with_capacity(DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2);

    for row in 0..n_rows {
        if row % DEFAULT_ROW_BUFFER_LEN == 0 {
            sender.send(buffer).expect("Bad buffer, send failed");
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
    sender.send(buffer).expect("Bad buffer, send failed");

    println!("thread {} done!", thread);
    drop(sender);
}

fn distribute_thread_workload(n_rows: usize, n_threads: usize) -> Vec<usize> {
    let thread_local_rows = (n_rows - 1) / n_threads + 1;
    let remaining_rows = thread_local_rows * n_threads - n_rows;
    let mut thread_workload: Vec<usize> = Vec::with_capacity(n_threads);
    for n in 0..n_threads {
        let spare = if n < remaining_rows { 1 } else { 0 };
        let rows_to_process = n + thread_local_rows - spare;
        thread_workload.insert(n, rows_to_process - n);
    }
    thread_workload
}
///
fn generate_threaded(schema: FixedSchema, n_rows: usize, n_threads: usize) {
    let worksize = distribute_thread_workload(n_rows, n_threads);

    let arc_schema = Arc::new(schema);

    let (sender, receiver) = channel::unbounded();

    let threads: Vec<_> = (0..n_threads)
        .map(|t| {
            let arc_clone = Arc::clone(&arc_schema);
            let sender_clone = sender.clone();
            let rows = worksize[t];
            thread::spawn(move || generate_from_thread(t, arc_clone, rows, sender_clone))
        })
        .collect();
    drop(sender);
    write_mock_file(&receiver);

    for handle in threads {
        handle.join().unwrap();
    }
}

fn write_mock_file(receiver: &channel::Receiver<Vec<u8>>) {
    let mut path = PathBuf::from(String::from("resources/mock_files/test_mock"));
    path.set_extension("flf");

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .unwrap();

    for buf in receiver {
        file.write_all(&buf).expect("Bad buffer, write failed!");
    }
}

///
#[allow(dead_code)]
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

    #[test]
    fn test_workload_distribution_16_threads_1000_rows() {
        let threads = 16;
        let rows = 1000;
        let workload = distribute_thread_workload(rows, threads);
        let row_sum: usize = workload.iter().sum();
        assert_eq!(rows, row_sum)
    }
}
