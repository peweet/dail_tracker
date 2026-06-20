"""Robust text decoding for Irish public-body tabular exports (CSV / raw text).

Irish gov.ie and local-authority CSV exports come in TWO encodings, with no declaration:
most are **Windows-1252 (cp1252)** — byte 0x80 is '€', 0x92 a smart apostrophe, 0xC9 'É',
the fadas in 0xC0–0xFF — but some are plain UTF-8. Reading a cp1252 file as UTF-8 (the old
``encoding="utf8-lossy"`` default) replaced every such byte with U+FFFD '�', irreversibly
destroying the character (verified 2026-06-20: TIA "Éamonn Conlon", Meath "O'Mahony", every
'€' sign). But blindly decoding everything as cp1252 would corrupt the genuinely-UTF-8 files
the OTHER way (a UTF-8 'é' = C3 A9 becomes "Ã©").

``decode_table_bytes`` resolves both without guessing wrong: try STRICT UTF-8 first — a real
UTF-8 file decodes cleanly and is returned unchanged — and only on a decode error fall back
to cp1252, because a cp1252 file almost always contains a byte sequence that is invalid as
UTF-8 (a lone high byte), so it lands in the fallback. This never regresses a file that
already decoded cleanly; it only repairs the ones that were being mangled.
"""

from __future__ import annotations


def decode_table_bytes(b: bytes) -> str:
    """Decode CSV/text bytes to ``str``, repairing cp1252 exports without harming UTF-8 ones.

    Strict UTF-8 first (clean files pass through identically to the old utf8-lossy path);
    cp1252 fallback for the Windows exports that fail strict UTF-8. cp1252's 5 undefined byte
    slots (0x81/8D/8F/90/9D) fall back to replacement — those are not real data.
    """
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("cp1252", errors="replace")
