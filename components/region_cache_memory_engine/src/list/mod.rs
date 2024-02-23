// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.


mod arena;
mod key;
mod list;
mod memory_control;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Bound<T> {
    /// An inclusive bound.
    Included(T),
    /// An exclusive bound.
    Excluded(T),
    /// An infinite endpoint. Indicates that there is no bound in this direction.
    Unbounded,
}
