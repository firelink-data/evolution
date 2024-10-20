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
// File created: 2023-12-11
// Last updated: 2024-10-20
//

use evolution_common::error::Result;

use std::sync::Arc;

/// A trait providing functions to read and slice buffered data.
pub trait Slicer<'a> {
    type Buffer: 'a;

    fn is_done(&self) -> bool;
    fn read_to_buffer(&mut self, buffer: Self::Buffer);
    fn seek_relative(&mut self, offset: i64);

    fn try_read_to_buffer(&mut self, buffer: Self::Buffer) -> Result<()>;
    fn try_seek_relative(&mut self, offset: i64) -> Result<()>;
}
///
pub type SlicerRef<'a, B> = Arc<dyn Slicer<'a, Buffer = B>>;
