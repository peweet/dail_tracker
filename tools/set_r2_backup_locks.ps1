<#
Set a narrow, retention-based R2 Bucket Lock policy for the data-backup archive.

The mutable live trees and manifests/latest.tsv are deliberately excluded: rclone sync
must replace them. Active Restic prefixes are also excluded because restic forget/prune
must delete unreachable packs. Only the version archive and immutable dated manifests
are locked against overwrite or deletion for the configured recovery window.

Prerequisite: set CLOUDFLARE_API_TOKEN to a short-lived Cloudflare API token with
Workers R2 Storage Write permission. An R2 S3 Object Read & Write credential cannot
call this REST API.
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$AccountId = 'dda75db5c9db02954a7b45e69052c742',
    [string]$Bucket = 'dail-tracker-backup',
    [ValidateRange(1, 3650)]
    [int]$RetentionDays = 45,
    [string]$ApiToken = $env:CLOUDFLARE_API_TOKEN
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ApiToken)) {
    throw 'Set CLOUDFLARE_API_TOKEN to a Cloudflare API token with Workers R2 Storage Write permission.'
}

$seconds = $RetentionDays * 24 * 60 * 60
$managedIds = @('dail-backup-versions', 'dail-backup-manifests')
$managedRules = @(
    [ordered]@{
        id = $managedIds[0]
        enabled = $true
        prefix = 'versions/'
        condition = [ordered]@{ type = 'Age'; maxAgeSeconds = $seconds }
    },
    [ordered]@{
        id = $managedIds[1]
        enabled = $true
        prefix = 'manifests/archive/'
        condition = [ordered]@{ type = 'Age'; maxAgeSeconds = $seconds }
    }
)

$endpoint = "https://api.cloudflare.com/client/v4/accounts/$AccountId/r2/buckets/$Bucket/lock"
$headers = @{ Authorization = "Bearer $ApiToken" }
$current = Invoke-RestMethod -Method Get -Uri $endpoint -Headers $headers
if (-not $current.success) { throw "Cloudflare rejected the read of bucket lock rules for $Bucket." }

# PUT replaces the complete rule set. Preserve any future independently-managed rule.
$existing = @($current.result.rules)
$unmanaged = @($existing | Where-Object { $_.id -notin $managedIds })
$body = @{ rules = @($unmanaged + $managedRules) } | ConvertTo-Json -Depth 8

if (-not $PSCmdlet.ShouldProcess("R2 bucket $Bucket", "apply $RetentionDays-day lock rules to versions/ and manifests/archive/")) {
    Write-Host "No change made. Would apply $RetentionDays-day retention to versions/ and manifests/archive/."
    exit 0
}
$updated = Invoke-RestMethod -Method Put -Uri $endpoint -Headers $headers -ContentType 'application/json' -Body $body
if (-not $updated.success) { throw "Cloudflare rejected the update of bucket lock rules for $Bucket." }

$verified = Invoke-RestMethod -Method Get -Uri $endpoint -Headers $headers
if (-not $verified.success) { throw "Cloudflare rejected the post-update read of bucket lock rules for $Bucket." }
$actual = @($verified.result.rules | Where-Object { $_.id -in $managedIds } | Select-Object id, enabled, prefix, condition)
if ($actual.Count -ne $managedRules.Count) { throw 'Post-update verification did not find both managed lock rules.' }

$actual | Format-Table -AutoSize
