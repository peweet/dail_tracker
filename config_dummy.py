# config_dummy.py
# Minimal config: only two subfolders, 'raw' and 'output' at the project root
from pathlib import Path
import pathlib
BASE_DIR = Path(__file__).resolve().parent
LOBBY_DIR = BASE_DIR / "lobbyist"
BILLS_DIR = BASE_DIR / "bills"
MEMBERS_DIR = BASE_DIR / "members"


RAW_DIR = str(BASE_DIR / "raw")
OUTPUT_DIR = str(BASE_DIR / "output")




# config.py
# from pathlib import Path
# from dataclasses import dataclass

# @dataclass(frozen=True)
# class Paths:
#     base:      Path
#     data:      Path
#     raw:       Path
#     processed: Path
#     output:    Path
#     logs:      Path

#     @classmethod
#     def from_base(cls, base: Path) -> "Paths":
#         return cls(
#             base      = base,
#             data      = base / "data",
#             raw       = base / "data" / "raw",
#             processed = base / "data" / "processed",
#             output    = base / "output",
#             logs      = base / "logs",
#         )

#     def ensure_dirs(self) -> None:
#         """Create all directories if they don't exist."""
#         for path in [self.data, self.raw, self.processed, self.output, self.logs]:
#             path.mkdir(parents=True, exist_ok=True)


# # Instantiate once, import everywhere
# PATHS = Paths.from_base(Path(__file__).resolve().parent)