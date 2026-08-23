//! Optional PyO3 acceleration kernels.
//!
//! This crate is intentionally narrow. Python owns policy and remains the
//! semantic oracle; the native functions only process owned, plain data.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use rayon::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use unicode_normalization::UnicodeNormalization;

static COMBINING_MARKS: std::ops::RangeInclusive<char> = '\u{0300}'..='\u{036f}';
static LEGAL_SUFFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b(?:THE|AND|LIMITED|LTD|DAC|PLC|CLG|UC|COMPANY|DESIGNATED ACTIVITY COMPANY|COMPANY LIMITED BY GUARANTEE|UNLIMITED COMPANY|GROUP|HOLDINGS|IRELAND|IRL|OF)\b",
    )
    .expect("the fixed legal-suffix pattern is valid")
});
static PUNCTUATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"[\.,&'\"]"#).expect("the fixed punctuation pattern is valid"));
static NON_ALNUM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[^A-Z0-9 ]").expect("the fixed non-alphanumeric pattern is valid")
});
static WHITESPACE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s+").expect("the fixed whitespace pattern is valid"));

/// Match `shared.name_norm.name_norm_str` for an already-coerced Python string.
fn name_norm_one(value: &str) -> String {
    let decomposed: String = value
        .nfd()
        .filter(|ch| !COMBINING_MARKS.contains(ch))
        .collect();
    let upper = decomposed.to_uppercase();
    let punctuation = PUNCTUATION.replace_all(&upper, " ");
    let suffixes = LEGAL_SUFFIX.replace_all(&punctuation, " ");
    let alphanumeric = NON_ALNUM.replace_all(&suffixes, " ");
    WHITESPACE.replace_all(&alphanumeric, " ").trim().to_owned()
}

/// Batch company-name normalisation over owned strings.
///
/// PyO3 converts the Python sequence before this function detaches from the
/// interpreter. The costly normalisation then touches Rust-owned data only, so
/// Python threads can run while it executes. Do not pass Python callbacks or
/// objects into this kernel: that would re-enter the interpreter and defeat the
/// GIL boundary this trial is meant to measure.
#[pyfunction(signature = (values, *, workers = 1))]
fn name_norm_many(py: Python<'_>, values: Vec<String>, workers: usize) -> PyResult<Vec<String>> {
    if workers == 0 {
        return Err(PyValueError::new_err("workers must be at least one"));
    }

    let result = py.detach(move || -> Result<Vec<String>, String> {
        if workers == 1 || values.len() < 2 {
            return Ok(values.iter().map(|value| name_norm_one(value)).collect());
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .map_err(|error| format!("could not create native worker pool: {error}"))?;
        Ok(pool.install(|| {
            values
                .par_iter()
                .map(|value| name_norm_one(value))
                .collect()
        }))
    });

    result.map_err(PyRuntimeError::new_err)
}

#[pymodule]
fn _dail_native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(name_norm_many, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::name_norm_one;

    #[test]
    fn normalises_known_join_keys() {
        assert_eq!(name_norm_one("Tirlán Ltd"), "TIRLAN");
        assert_eq!(name_norm_one("Turner & Townsend"), "TURNER TOWNSEND");
        assert_eq!(name_norm_one("Acme Holdings Limited"), "ACME");
    }
}
