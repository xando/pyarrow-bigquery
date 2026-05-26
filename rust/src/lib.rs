// pyarrow-bigquery Rust acceleration module.

use pyo3::prelude::*;

mod proto;
mod reader;

#[pyfunction]
fn build_info() -> &'static str {
    concat!("pyarrow_bigquery_rs ", env!("CARGO_PKG_VERSION"))
}

#[pymodule]
fn _rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(build_info, m)?)?;
    m.add_class::<reader::PyReader>()?;
    Ok(())
}
