# CSS Reuse Guide

Your existing `utility/styles/base.css` already has the right direction: newsroom-light tokens, Streamlit container styling, sidebar border, metric cards, dataframe wrappers, hero classes, badges, callouts, provenance boxes, and mobile adjustments.

Claude should reuse it before adding CSS.

## Existing class vocabulary

- `dt-hero`
- `dt-kicker`
- `dt-dek`
- `dt-badge`
- `dt-callout`
- `dt-provenance-box`

## Good CSS changes

- extend shared `base.css`
- use semantic class names
- use existing `--dt-*` variables
- keep the civic editorial tone

## Bad CSS changes

- inline a new global `<style>` block in every page
- add a new theme system
- add decorative animations
- use custom JavaScript
