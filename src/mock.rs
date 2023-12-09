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
* Last updated: 2023-12-09
*/

use crate::schema::{self, FixedColumn};
use log::{debug, info};
use rand::distributions::{Alphanumeric, DistString};
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;
use std::thread;

pub(crate) static MOCKED_FILENAME_LEN: usize = 16;

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

    pub fn kgenerate(&self, n_rows: usize) {
        for row in 0..n_rows {
            let mut buffer: Vec<u8> = Vec::new();
            
            for col in self.schema.iter() {
                let mocked_values: Vec<u8> = col.mock();
            }
        }
    }

    /// TODO: randomize data based on dtype
    pub fn generate(&self, n_rows: usize) {
        let now = SystemTime::now();

        let mut rows: Vec<u8> = Vec::new();
        for _row in 0..n_rows {
            let mut row_builder: Vec<u8> = Vec::new();
            for col in self.schema.iter() {
                let col_len = col.length();
                let mocked_values: Vec<u8> =
                    (0..col_len).map(|_| "a").collect::<String>().into_bytes();
                row_builder.extend_from_slice(mocked_values.as_slice());
            }
            row_builder.extend_from_slice("\rn\n".as_bytes());
            rows.extend_from_slice(row_builder.as_slice());
        }

        info!(
            "Produced {} rows in {}ms",
            n_rows,
            now.elapsed().unwrap().as_millis(),
        );

        let mut path =
            PathBuf::from(Alphanumeric.sample_string(&mut rand::thread_rng(), MOCKED_FILENAME_LEN));

        path.set_extension("flf");
        fs::write(path, rows).unwrap();
    }
}

pub trait Mock {
    fn mock(&self) -> Vec<u8>;
}

pub(crate) fn mock_bool(len: usize) -> Vec<u8> {
}

///
pub(crate) fn mock_from_schema(schema_path: String, n_rows: usize) {
    let schema = schema::FixedSchema::from_path(schema_path.into());
    let mocker = FixedMocker::new(schema);
    //mocker.generate(n_rows);
    //mocker.generate_threaded(n_rows, 5);
    mocker.kgenerate(n_rows);
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
