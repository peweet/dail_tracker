# Page Brief Template

Fill this before asking Claude to create a contract.

## Page name

## One-sentence user promise

## Primary users

- General public:
- Journalists/researchers:
- Maintainers:

## Top questions the page must answer

1.
2.
3.
4.
5.

## Expected analytical view

View name:

Grain:

Examples:
- one row per member
- one row per member per year
- one row per declared interest
- one row per member per sitting
- one row per lobbying interaction

## Required fields

```text
member_id
member_name
party_name
constituency
...
```

## Filters

```text
party_name: essential
constituency: essential
year: essential
free_text_search: optional
```

## Metrics

Only include metrics that are already supplied by a view or are pure UI counts.

```text
visible_rows: UI count OK
total_interests: pipeline summary view
average_attendance: pipeline summary view
```

## Charts

For each chart, state the question it answers and whether the pipeline already shapes the data.

## Table behavior

- Default sort:
- Key columns first:
- Columns hidden by default:
- CSV export: yes/no

## Empty states

What should the user see when no records match?

## Provenance/caveats

What source/freshness/caveat should be visible?

## Things Claude must not build

- joins in Streamlit
- metric definitions in Streamlit
- features not listed here
- new global CSS system
- custom JavaScript
