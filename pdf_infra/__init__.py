"""Shared PDF acquisition infrastructure: source polling, historical URL
catch-up download, and endpoint health-checking.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). The poller
and downloader are run by the bootstrap chain via ``python -m pdf_infra.<step>``
so the repo root (cwd) is on ``sys.path`` and ``import config`` resolves;
``pdf_endpoint_check`` is also run standalone by the nightly endpoint-health CI
job (``python -m pdf_infra.pdf_endpoint_check``).
"""
