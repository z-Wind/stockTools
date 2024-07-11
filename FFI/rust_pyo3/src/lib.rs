#![warn(
clippy::pedantic,
missing_copy_implementations,
missing_debug_implementations,
//missing_docs,
rustdoc::broken_intra_doc_links,
trivial_numeric_casts,
unused_allocation
)]
#![allow(
    clippy::missing_errors_doc,
    clippy::implicit_hasher,
    clippy::similar_names,
    clippy::module_name_repetitions
)]

mod stock;

use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn rust_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<stock::Price>()?;
    m.add_class::<stock::Stat>()?;
    m.add_class::<stock::Stock>()?;
    Ok(())
}
