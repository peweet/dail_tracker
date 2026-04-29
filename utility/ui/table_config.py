"""Column config factories for Dáil Tracker vote tables."""
from __future__ import annotations
import streamlit as st


def vote_index_column_config() -> dict:
    return {
        "vote_id":         st.column_config.TextColumn("ID", width="small"),
        "vote_date":       st.column_config.DateColumn("Date", format="D MMM YYYY", width="small"),
        "debate_title":    st.column_config.TextColumn("Debate / Subject", width="large"),
        "vote_outcome":    st.column_config.TextColumn("Outcome", width="small"),
        "yes_count":       st.column_config.NumberColumn("Yes ✓", format="%d", width="small"),
        "no_count":        st.column_config.NumberColumn("No ✗", format="%d", width="small"),
        "abstained_count": st.column_config.NumberColumn("Abs", format="%d", width="small"),
        "margin":          st.column_config.NumberColumn("Margin", format="%d", width="small"),
    }


def member_detail_column_config() -> dict:
    return {
        "member_name":  st.column_config.TextColumn("TD", width="medium"),
        "party_name":   st.column_config.TextColumn("Party", width="small"),
        "constituency": st.column_config.TextColumn("Constituency", width="medium"),
        "vote_type":    st.column_config.TextColumn("Vote", width="small"),
    }


def td_summary_column_config() -> dict:
    return {
        "member_name":    st.column_config.TextColumn("TD", width="medium"),
        "party_name":     st.column_config.TextColumn("Party", width="small"),
        "constituency":   st.column_config.TextColumn("Constituency", width="medium"),
        "division_count": st.column_config.NumberColumn("Divisions", format="%d", width="small"),
        "yes_count":      st.column_config.NumberColumn("Yes ✓", format="%d", width="small"),
        "no_count":       st.column_config.NumberColumn("No ✗", format="%d", width="small"),
    }


def td_history_column_config() -> dict:
    return {
        "vote_date":    st.column_config.DateColumn("Date", format="D MMM YYYY", width="small"),
        "debate_title": st.column_config.TextColumn("Debate / Subject", width="large"),
        "vote_type":    st.column_config.TextColumn("Vote", width="small"),
        "vote_outcome": st.column_config.TextColumn("Outcome", width="small"),
    }
