# SQLite connection lifetime before archive cleanup

Confirmed: 2026-10-10 on Windows.

`with sqlite3.connect(path) as connection` manages commit and rollback, but does not close
the connection. A retained handle can prevent later rename or temporary-directory cleanup,
including after an otherwise successful archive build. Use `contextlib.closing` or an explicit
`try/finally: connection.close()` when a reader must release a file before sealing or cleanup.

Evidence: the real private QGIS archive delivery test failed with a Windows sharing violation
after reading the generated GeoPackage; explicitly closing the reader repaired the same test.
The public lesson is file-handle lifetime, not authority to remove or bypass a locked file.

Validate the complete archive/download/cleanup seam as well as the SQLite query result. Do not
weaken cleanup or customer-delivery checks to hide an open reader.
