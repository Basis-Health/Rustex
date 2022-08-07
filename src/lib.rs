mod rustex;

use simple_logger::SimpleLogger;
use log::LevelFilter;

use pyo3::prelude::*;
use rustex::Rustex;

/// enable_logging(verbose: bool, /)
/// --
///
/// Enables logging of the Rustex library
#[pyfunction]
fn enable_logging(verbose: bool) {
    SimpleLogger::default().with_level(if verbose {LevelFilter::Trace } else {LevelFilter::Debug}).init().unwrap();
}

#[pymodule]
fn rustex(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Rustex>()?;
    m.add_function(wrap_pyfunction!(enable_logging, m)?)?;

    Ok(())
}