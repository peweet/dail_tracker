from __future__ import annotations

from pathlib import Path

from tools.evals.cleanroom import MARKER, prepare_cleanroom, should_copy, validate_cleanroom


def test_cleanroom_path_filter_excludes_scorers_git_and_private_overlay():
    assert should_copy("AGENTS.md")
    assert should_copy("utility/pages_code/home.py")
    assert not should_copy("tools/evals/harness_bench.py")
    assert not should_copy("test/tools/evals/test_cleanroom.py")
    assert not should_copy("planning/product/private.md")
    assert not should_copy(".git/config")
    assert not should_copy("../answer-key.json")


def test_cleanroom_is_a_copy_with_a_validated_lifetime(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "AGENTS.md").write_text("safe guidance\n", encoding="utf-8")
    scorer = source / "tools" / "evals" / "answer.py"
    scorer.parent.mkdir(parents=True)
    scorer.write_text("ANSWER = 42\n", encoding="utf-8")

    clean_path: Path | None = None
    with prepare_cleanroom(source, files=["AGENTS.md", "tools/evals/answer.py"]) as cleanroom:
        clean_path = cleanroom
        metadata = validate_cleanroom(cleanroom)
        assert (cleanroom / "AGENTS.md").read_text(encoding="utf-8") == "safe guidance\n"
        assert not (cleanroom / "tools" / "evals").exists()
        assert not (cleanroom / ".git").exists()
        assert (cleanroom / MARKER).is_file()
        assert metadata["files_copied"] == 1

    assert clean_path is not None and not clean_path.exists()
