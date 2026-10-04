# CodeQL ownership

The public repository uses GitHub CodeQL default setup, configured in
`Settings → Security → Code scanning`. Verified on 2026-10-04: its default query suite
successfully analyzed Actions, JavaScript/TypeScript, Python and Rust for commit `02cd87e70`
([successful default run](https://github.com/peweet/dail_tracker/actions/runs/37209037862)).

Do not add a competing advanced CodeQL workflow while default setup is enabled. GitHub rejected
both uploads from the former `.github/workflows/codeql.yml` for that reason
([failed advanced run](https://github.com/peweet/dail_tracker/actions/runs/37209038736)), so that
workflow was retired. This preserves the currently successful default coverage; it does not
claim the former workflow's additional `security-and-quality` queries run in default setup.
If those queries are required, migrate deliberately to advanced setup, retaining Actions and
Rust alongside Python and JavaScript/TypeScript before disabling default setup.
