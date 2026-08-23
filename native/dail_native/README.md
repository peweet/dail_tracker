# dail-native trial

This is an optional, local PyO3 package. It is not part of the deployed
application dependency set and no live extractor selects it by default.

Its first kernel is `name_norm_many`: a batch version of the established
`shared.name_norm.name_norm_str` company-name join key. The Rust function owns
its input strings and uses `Python::detach` while it performs native work, so
independent Python threads can make progress. `workers=1` is the default;
parallel Rust work must be explicitly requested and capped to the actual CPU
quota by the caller.

## Build locally

Use stable Rust and the project virtual environment. Maturin uses `VIRTUAL_ENV`
to select the Python environment; clear a simultaneously inherited Conda marker
before building:

```powershell
$env:RUSTUP_TOOLCHAIN = "stable-x86_64-pc-windows-msvc"
$env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
$env:VIRTUAL_ENV = (Resolve-Path ".venv").Path
Remove-Item Env:CONDA_PREFIX -ErrorAction SilentlyContinue
Set-Location native/dail_native
C:\Users\pglyn\.local\bin\uv.exe tool run --from "maturin>=1.8,<2" maturin develop --release
```

Then run the contract and benchmark checks:

```powershell
.venv\Scripts\python.exe -m pytest test/native/test_name_norm_native.py test/shared/test_name_norm.py -q
.venv\Scripts\python.exe tools/benchmark_native_name_norm.py --limit 50000 --workers 4
```

Promotion requires the native result to remain byte-identical to the Python
oracle and an end-to-end, deployment-shape profile that shows a worthwhile
p95 improvement. Never use a benchmark result alone to alter join semantics.
