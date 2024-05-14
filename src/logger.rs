//
// MIT License
//
// Copyright (c) 2024 Firelink Data
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// File created: 2023-11-21
// Last updated: 2024-05-15
//

use chrono::Local;
use colored::Colorize;
use log::{Level, Log, Metadata, Record, SetLoggerError};

use std::env;

pub(crate) const DEFAULT_LOG_LEVEL: Level = Level::Warn;

/// Get the [`Level`] from the environment variable `RUST_LOG`, default to [`DEFAULT_LOG_LEVEL`].
fn get_log_level_from_env() -> Level {
    match env::var("RUST_LOG") {
        Ok(val) => match val.to_uppercase().as_str() {
            "TRACE" => Level::Trace,
            "DEBUG" => Level::Debug,
            "INFO" => Level::Info,
            "INFORMATION" => Level::Info,
            "WARN" => Level::Warn,
            "WARNING" => Level::Warn,
            "ERR" => Level::Error,
            "ERROR" => Level::Error,
            &_ => DEFAULT_LOG_LEVEL,
        },
        Err(_) => DEFAULT_LOG_LEVEL,
    }
}

/// A wrapper struct for the env-logger.
pub struct Logger {
    log_level: Level,
}

impl Logger {
    /// Create a new [`Logger`] with associated [`Level`].
    pub fn new(log_level: Level) -> Self {
        Self { log_level }
    }

    fn trace(&self, record: &Record) {
        println!(
            "[{}]  {}\t {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            record.args(),
        );
    }

    fn debug(&self, record: &Record) {
        println!(
            "[{}]  {}\t {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level().as_str().blue(),
            record.args(),
        );
    }

    fn info(&self, record: &Record) {
        println!(
            "[{}]  {}\t {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level().as_str().green(),
            record.args(),
        );
    }

    fn warn(&self, record: &Record) {
        println!(
            "[{}]  {}\t {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level().as_str().yellow(),
            record.args(),
        );
    }

    /// Here we can use [`.to_string()`] because an error log should only be used
    /// when terminating execution due to error, and then memory allocations
    /// and performance is not an issue. We can be sloppy here.
    fn error(&self, record: &Record) {
        println!(
            "[{}]  {}\t {}",
            Local::now()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
                .red()
                .bold(),
            record.level().as_str().red().bold(),
            record.args().to_string().red().bold(),
        );
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.log_level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            match record.level() {
                Level::Trace => self.trace(record),
                Level::Debug => self.debug(record),
                Level::Info => self.info(record),
                Level::Warn => self.warn(record),
                Level::Error => self.error(record),
            };
        };
    }

    // This does not need to be implemented, just empty boilerplate for trait.
    fn flush(&self) {}
}

/// Try and setup the env-logger from environment variable.
///
/// # Errors
/// If we can not set the global logger to our newly created env-logger.
pub(crate) fn try_init_logging() -> Result<(), SetLoggerError> {
    let log_level = get_log_level_from_env();
    let logger = Logger::new(log_level);
    log::set_boxed_logger(Box::new(logger))
        .map(|()| log::set_max_level(log_level.to_level_filter()))
}