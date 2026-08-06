param(
    [switch]$Deploy
)

$ErrorActionPreference = "Stop"
$appRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$repoRoot = Resolve-Path (Join-Path $appRoot "..\..")
$snapshotPath = Join-Path $appRoot "public\_private\procurement-snapshot.json"

Set-Location -LiteralPath $repoRoot
& uv run --no-sync python apps/public-signal/private-api/build_snapshot.py --output $snapshotPath
if ($LASTEXITCODE -ne 0) { throw "Snapshot build failed." }

$snapshot = Get-Content -LiteralPath $snapshotPath -Raw -Encoding utf8 | ConvertFrom-Json
if ($snapshot.schema -ne "publicsignal-procurement-snapshot/1") { throw "Unexpected snapshot schema." }
$noticeCount = @($snapshot.feed.opportunities).Count
if ($noticeCount -le 200) { throw "Snapshot row-loss guard failed: only $noticeCount opportunities were built." }
foreach ($contractName in @("sectors", "buyers", "suppliers")) {
    if ($snapshot.contracts.$contractName.status -ne "reviewed") { throw "The $contractName contract is not reviewed." }
}

Set-Location -LiteralPath $appRoot
& npm run check
if ($LASTEXITCODE -ne 0) { throw "PublicSignal checks failed." }
& npx wrangler deploy --dry-run
if ($LASTEXITCODE -ne 0) { throw "Wrangler dry run failed." }

Write-Host "Validated $noticeCount opportunities built at $($snapshot.built_at)."
if ($Deploy) {
    & npx wrangler deploy
    if ($LASTEXITCODE -ne 0) { throw "Cloudflare deployment failed." }
    Write-Host "PublicSignal deployed. Verify /api/health before inviting beta users."
} else {
    Write-Host "Dry run complete. Re-run with -Deploy after reviewing the snapshot timestamp and counts."
}
