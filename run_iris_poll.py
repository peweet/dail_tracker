"""Click-and-run: poll Iris Oifigiúil for new Tue/Fri PDFs since last ingestion.

Wraps iris_oifigiuil_poller.main() with default args so a double-click or
`python run_iris_poll.py` triggers the standard 10-day lookback against
data/bronze/iris_oifigiuil/.

Run:
    python run_iris_poll.py

Exit codes:
    0 — clean (anything new was downloaded and validated)
    1 — at least one download was attempted but failed (network / signature / size)
    2 — past Tue/Fri returned 4xx on every URL variant — slug pattern may have shifted
"""

import sys

from iris_oifigiuil_poller import main

sys.exit(main([]))
