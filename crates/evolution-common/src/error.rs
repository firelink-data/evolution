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
// File created: 2024-02-05
// Last updated: 2024-05-28
//

use std::error;
use std::fmt;
use std::result;

///
pub type Result<T> = result::Result<T, Box<dyn error::Error>>;

/// An error representing that something went wrong when setting up all resources.
/// Most likely caused by some I/O error due to incorrect paths or write/read modes.
#[derive(Debug)]
pub struct SetupError {
    details: String,
}

impl SetupError {
    /// Create a new [`SetupError`] which will display the provided message.
    pub fn new(msg: &str) -> Self {
        Self {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl error::Error for SetupError {
    fn description(&self) -> &str {
        &self.details
    }
}

/// An error representing that something went wrong during execution.
#[derive(Debug)]
pub struct ExecutionError {
    details: String,
}

impl ExecutionError {
    /// Create a new [`ExecutionError`] which will display the provided message.
    pub fn new(msg: &str) -> Self {
        Self {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl error::Error for ExecutionError {
    fn description(&self) -> &str {
        &self.details
    }
}

#[cfg(test)]
mod tests_error {
    use super::*;

    #[test]
    fn test_setup_error() {
        assert_eq!(
            "uh oh stinky something went wrong!",
            SetupError::new("uh oh stinky something went wrong!")
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn test_execution_error() {
        assert_eq!(
            "uh oh stinky something went wrong!",
            ExecutionError::new("uh oh stinky something went wrong!")
                .to_string()
                .as_str(),
        );
    }
}
