"""Column config factories for Dáil Tracker vote tables."""

from __future__ import annotations

import streamlit as st


def vote_index_column_config() -> dict:
    return {
        "vote_id": st.column_config.TextColumn("ID", width="small"),
        "vote_date": st.column_config.DateColumn("Date", format="D MMM YYYY", width="small"),
        "debate_title": st.column_config.TextColumn("Debate / Subject", width="large"),
        "vote_outcome": st.column_config.TextColumn("Outcome", width="small"),
        "yes_count": st.column_config.NumberColumn("Yes ✓", format="%d", width="small"),
        "no_count": st.column_config.NumberColumn("No ✗", format="%d", width="small"),
        "abstained_count": st.column_config.NumberColumn("Abs", format="%d", width="small"),
        "margin": st.column_config.NumberColumn("Margin", format="%d", width="small"),
    }


def member_detail_column_config() -> dict:
    return {
        "member_name": st.column_config.TextColumn("TD", width="medium"),
        "party_name": st.column_config.TextColumn("Party", width="small"),
        "constituency": st.column_config.TextColumn("Constituency", width="medium"),
        "vote_type": st.column_config.TextColumn("Vote", width="small"),
    }


def td_summary_column_config() -> dict:
    return {
        "member_name": st.column_config.TextColumn("TD", width="medium"),
        "party_name": st.column_config.TextColumn("Party", width="small"),
        "constituency": st.column_config.TextColumn("Constituency", width="medium"),
        "division_count": st.column_config.NumberColumn("Divisions", format="%d", width="small"),
        "yes_count": st.column_config.NumberColumn("Yes ✓", format="%d", width="small"),
        "no_count": st.column_config.NumberColumn("No ✗", format="%d", width="small"),
    }


def td_history_column_config() -> dict:
    return {
        "vote_date": st.column_config.DateColumn("Date", format="D MMM YYYY", width="small"),
        "debate_title": st.column_config.TextColumn("Debate / Subject", width="large"),
        "vote_type": st.column_config.TextColumn("Vote", width="small"),
        "vote_outcome": st.column_config.TextColumn("Outcome", width="small"),
    }


def committee_roster_column_config(member_label: str = "TD") -> dict:
    # P1-3 + P2-6 audit fixes:
    # - "Member" replaces "TD" / "Senator" as the header label — keeps the
    #   parent chamber pill carrying that context and reads cleanly for
    #   mixed-chamber audiences. ``member_label`` is preserved as the
    #   default for callers that still pass a value but is no longer the
    #   default rendered header.
    # - Party column bumped from "small" to "medium" so labels like
    #   "Social Democrats" and "Independent Ireland" stop truncating to
    #   "Social Dem" / "Independer".
    _ = member_label  # accepted for backwards compatibility; not rendered
    return {
        "name": st.column_config.TextColumn("Member", width="medium"),
        "party": st.column_config.TextColumn("Party", width="medium"),
        "constituency": st.column_config.TextColumn("Constituency", width="medium"),
        "role": st.column_config.TextColumn("Role", width="medium"),
        "is_chair": st.column_config.CheckboxColumn("Chair", width="small"),
        "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD", width="small"),
        "end": st.column_config.DateColumn("End", format="YYYY-MM-DD", width="small"),
    }


def committee_membership_column_config() -> dict:
    return {
        "committee": st.column_config.TextColumn("Committee", width="large"),
        "committee_url": st.column_config.LinkColumn("Link", display_text="Open ↗", width="small"),
        "type": st.column_config.TextColumn("Type", width="small"),
        "role": st.column_config.TextColumn("Role", width="medium"),
        "is_chair": st.column_config.CheckboxColumn("Chair", width="small"),
        "status": st.column_config.TextColumn("Status", width="small"),
        "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD", width="small"),
        "end": st.column_config.DateColumn("End", format="YYYY-MM-DD", width="small"),
    }
