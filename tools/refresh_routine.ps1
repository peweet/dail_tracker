# tools/refresh_routine.ps1 — repeatable refresh of the chains we run most often.
#
# Thin wrapper over tools/refresh.ps1 that hardcodes the usual chain selection so
# you don't have to remember it:
#     payments, attendance, lobbying, iris, source_health
#
# Like refresh.ps1, this runs the pipeline (those chains only) then the gated
# publish (commit + push -> Streamlit Cloud redeploys). The publish is gated on
# the data-integrity checks, so a broken/partial ETL can never reach the live app.
#
# Usage:
#   .\tools\refresh_routine.ps1              # run the chains, then publish
#   .\tools\refresh_routine.ps1 --dry-run    # run the chains, PREVIEW publish (no commit)
#   .\tools\refresh_routine.ps1 --no-push    # run + commit locally, do not push
#
# Any extra args pass straight through to refresh.ps1 -> refresh.py.

$ErrorActionPreference = "Stop"
# NB: the selection must be a single quoted string — unquoted, PowerShell parses the
# commas as an array and splats each chain as a separate argument, which argparse rejects.
& "$PSScriptRoot\refresh.ps1" --select "payments,attendance,lobbying,iris,source_health" @args
exit $LASTEXITCODE
