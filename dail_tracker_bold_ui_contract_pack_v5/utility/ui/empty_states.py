from __future__ import annotations

from utility.ui.components import empty_state


def no_records_for_filters() -> None:
    empty_state(
        "No records match these filters",
        "Try widening the date or year range, clearing a filter, or checking whether the source data has been processed.",
    )


def missing_registered_view(view_name: str) -> None:
    empty_state(
        "This view is not available yet",
        f"TODO_PIPELINE_VIEW_REQUIRED: registered analytical view `{view_name}`",
    )
