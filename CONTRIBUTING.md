# Contributing to Dáil Tracker

Thanks for your interest. Issues and pull requests are welcome.

## Licensing of contributions (please read first)

Dáil Tracker is **dual-licensed**: it is published under the [AGPL-3.0](LICENSE)
and also offered under a separate [commercial licence](COMMERCIAL-LICENSE.md).
So that the project can keep offering both, **all contributions are accepted
under the project's [Contributor Licence Agreement (`CLA.md`)](CLA.md)**.

- You keep the copyright in your contribution.
- You grant the maintainer the right to license your contribution under **both**
  the AGPL and the commercial licence.
- **By opening a pull request you confirm you have read and agree to the CLA.**
  For first-time contributors, the CLA bot (or a one-line confirmation in your
  PR) records this. Contributions from organisations may need an Entity CLA —
  see [`CLA.md`](CLA.md).

If you can't agree to the CLA, you're still very welcome to **file issues**,
suggest changes, or maintain your own AGPL fork.

## Reporting data issues

For data problems (a wrong figure, a missed match, a stale source), open an issue
with:

- the **source link** the data came from;
- the **expected** and **observed** values; and
- the affected **dataset or script**.

## Code contributions

- Set up the dev environment: `uv sync --extra pipeline --group dev`.
- Run the checks before pushing:
  ```bash
  uv run pytest test/ -v
  uv run ruff check .
  python tools/check_streamlit_logic_firewall.py
  ```
- Keep the project conventions (see `CLAUDE.md`): Polars for ETL, pandas only in
  the UI layer; Streamlit pages carry no business logic; never sum the three
  money grains; reuse the existing name-normalisation join key.
- Don't add third-party code under a licence incompatible with the AGPL or with
  the project's ability to relicense commercially.

## Questions

Open an issue, or for licensing/commercial questions email **{{LICENSING_EMAIL}}**.
