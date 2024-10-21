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
// File created: 2024-05-07
// Last updated: 2024-10-21
//

use evolution_common::error::Result;

/// A trait providing functions to build a specific column from a buffer.
pub trait ColumnBuilder: Send + Sync {
    type Output;
    
    fn try_build_column(&mut self, bytes: &[u8]) -> Result<usize>;
    fn finish(&mut self) -> Self::Output;
}
/// A short-hand notation for a generic [`ColumnBuilder`] reference.
pub type ColumnBuilderRef<T> = Box<dyn ColumnBuilder<Output = T>>;

/// A trait providing functions to build structured data from a buffer.
pub trait Builder: Send + Sync {
    type Buffer;
    type Output;

    fn build_from(&mut self, buffer: Self::Buffer);
    fn try_build_from(&mut self, buffer: Self::Buffer) -> Result<()>;
    fn try_finish(&mut self) -> Result<Self::Output>;
}
/// A short-hand notation for a generic [`Builder`] reference.
pub type BuilderRef<T, V> = Box<dyn Builder<Buffer = T, Output = V>>;