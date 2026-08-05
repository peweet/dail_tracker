from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.check_agent_context import main as check_agent_context  # noqa: E402


def main() -> int:
    """Compatibility entry point; the repository-wide ratchet is canonical."""
    return check_agent_context(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
