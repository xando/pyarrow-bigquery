// pyarrow-bigquery Rust acceleration module.
//
// Scaffolding only at this point — the module is imported by Python code in
// `pyarrow.bigquery` to confirm the build works end-to-end. Real
// implementations (per-stream BigQuery Storage reader, Arrow rebatcher) will
// land here in subsequent commits.

use pyo3::prelude::*;

#[pyfunction]
fn build_info() -> &'static str {
    concat!("pyarrow_bigquery_rs ", env!("CARGO_PKG_VERSION"))
}

#[pymodule]
fn _rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(build_info, m)?)?;
    Ok(())
}
