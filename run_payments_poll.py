"""Click-and-run: poll the payments topic for new PDFs.

Just runs the payments source from oireachtas_pdf_poller and prints the
result. Use this when you want to test/trigger the payments poll locally
without invoking the full pipeline.

Run:
    python run_payments_poll.py

Exit codes match oireachtas_pdf_poller:
    0 — clean
    1 — infra failure
    2 — HTML drift (selectors or filename hint may need updating)
"""

import json
import logging
import sys

from oireachtas_pdf_poller import SOURCES, _exit_code, run_one

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

result = run_one(SOURCES["payments"])
print(json.dumps(result, indent=2))
sys.exit(_exit_code({"payments": result}))
