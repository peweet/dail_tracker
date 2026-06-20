"""Unit tests for shared.text_encoding.decode_table_bytes — the cp1252/UTF-8 repair used
by the procurement CSV readers. Pure, no files; runs in the default CI lane."""

from shared.text_encoding import decode_table_bytes

# The real strings that were being destroyed in the gold layer (cp1252-encoded sources).
SAMPLES = ["Éamonn Conlon SC", "O'Mahony Pike Architects", "Signs Programme – works", "Value (€)", "Scoil Bhríde"]


def test_recovers_cp1252_encoded_text():
    for s in SAMPLES:
        assert decode_table_bytes(s.encode("cp1252")) == s


def test_passes_genuine_utf8_through_unchanged():
    # the critical no-regression property: a real UTF-8 file must NOT be re-decoded as cp1252
    for s in SAMPLES:
        assert decode_table_bytes(s.encode("utf-8")) == s


def test_plain_ascii_is_identity():
    assert decode_table_bytes(b"Supplier,Amount,Date") == "Supplier,Amount,Date"


def test_no_replacement_char_for_known_cp1252_bytes():
    # the bug signature was U+FFFD '�'; the repaired output must never contain it for these bytes
    assert "�" not in decode_table_bytes("Éamonn — €5".encode("cp1252"))


def test_undefined_cp1252_byte_falls_back_to_replacement_not_crash():
    # 0x81 is one of cp1252's 5 undefined slots; must degrade gracefully, never raise
    out = decode_table_bytes(b"abc\x81def")
    assert isinstance(out, str) and "abc" in out and "def" in out
