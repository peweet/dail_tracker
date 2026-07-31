"""
Member Overview — single-politician public accountability record.

Stage 1: Browse all TDs (identity columns; mart pending)
Stage 2: Full accountability profile — Attendance, Votes, Payments, Lobbying, Legislation

Entry points: row click, sidebar selectbox, ?member=join_key URL param

TODO_PIPELINE_VIEW_REQUIRED: v_member_overview_browse
  Pending columns: attendance_rate, payment_total_eur, declared_interests_count,
  lobbying_interactions_count, revolving_door_flag, government_status

Package map (mechanical split of the former 2,498-line member_overview.py, 2026-07):
  * _shared.py    — import header, constants, cached data-retrieval wrappers over
                     dail_tracker_core.queries.member_overview (used across every
                     other module in the package).
  * sections.py   — the `_section_*` profile-body renderers (legislation,
                     ministerial roles, statutory instruments, questions, debates,
                     committees) plus their private helpers.
  * browse.py     — `_render_browse` (Stage 1 — the all-members grid) and its
                     party/term/search filter helpers.
  * overview.py   — the `_ov_*` overview-card builders + `_render_overview`
                     (Stage 2 default landing) + `_render_pay_summary_tiles`.
  * profile.py    — `_render_stage2` (the full profile render, section router
                     included), the profile nav / section-switcher chrome, and
                     the constituency-context / contact-details blocks.
  * page.py       — `member_overview_page` — the ``?member=`` / session-state
                     routing between Stage 1 (browse) and Stage 2 (profile).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from .page import member_overview_page

__all__ = ["member_overview_page"]
