# Native exit status with PowerShell stderr redirection

Confirmed: 2026-10-10 on Windows PowerShell in the local execution harness.

Redirecting a native program's stderr with `*> log` can produce `NativeCommandError` records and
leave `$?` false even when `$LASTEXITCODE` is zero. A wrapper that reports the PowerShell status
can therefore label a completed diagnostic as failed because of normal stderr logging.

Capture `$LASTEXITCODE` immediately after the native command, print the captured value, and
explicitly `exit` with that value. For Python gates, logging to a UTF-8 file inside the process
also avoids PowerShell's conversion of native stderr into error records. Preserve both the
process status and the artifact checks; an artifact receipt alone is not process-exit evidence.

Evidence: a minimal Python probe wrote one stderr line and exited zero; PowerShell reported
`shell_success=False native_exit_code=0`. The initial private customer-PDF diagnostic wrote two
passing case receipts but its shell wrapper returned one. Its confirmation uses Python-side
logging and explicit native exit propagation. This is a wrapper diagnosis, not permission to
ignore a non-zero native exit or an actual failed artifact check.
