<#
.SYNOPSIS
  Install the checksum-pinned RTK pilot binary inside this repository's ignored cache.

.DESCRIPTION
  Downloads RTK v0.44.2 for native Windows, verifies both the release archive and
  extracted executable, and installs it at .cache/rtk/v0.44.2/rtk.exe.

  This script deliberately does not run `rtk init`, edit PATH, install hooks, or
  write global RTK configuration.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File tools/install_rtk.ps1
#>
[CmdletBinding()]
param()

Set-StrictMode -Version 3.0
$ErrorActionPreference = 'Stop'

$rtkVersion = '0.44.2'
$archiveSha256 = '3a1e114edce9080f8a10663e9c87488363a82f14a5ca8aab2ad416817f89d47c'
$executableSha256 = '60640b970fdf10451813ab4d9d24deb5c6370e43a5192eb14c6ba101a15b633c'
$assetName = 'rtk-x86_64-pc-windows-msvc.zip'
$assetUrl = "https://github.com/rtk-ai/rtk/releases/download/v$rtkVersion/$assetName"

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$installDir = Join-Path $repoRoot ".cache\rtk\v$rtkVersion"
$rtkExe = Join-Path $installDir 'rtk.exe'

function Get-Sha256Lower {
    param([Parameter(Mandatory = $true)][string]$Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Assert-RtkVersion {
    param([Parameter(Mandatory = $true)][string]$Path)

    $reportedVersion = (& $Path --version 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "RTK version check failed with exit code $LASTEXITCODE."
    }
    if ($reportedVersion -ne "rtk $rtkVersion") {
        throw "Expected 'rtk $rtkVersion' but the executable reported '$reportedVersion'."
    }
}

if (Test-Path -LiteralPath $rtkExe -PathType Leaf) {
    $installedHash = Get-Sha256Lower -Path $rtkExe
    if ($installedHash -ne $executableSha256) {
        throw "Refusing to replace unexpected file at $rtkExe (SHA-256 $installedHash)."
    }
    Assert-RtkVersion -Path $rtkExe
    Write-Host "RTK v$rtkVersion is already installed and verified at $rtkExe"
    exit 0
}

$tempDir = Join-Path ([IO.Path]::GetTempPath()) ("dail-extractor-rtk-" + [Guid]::NewGuid().ToString('N'))
$stagedExe = $null

try {
    New-Item -ItemType Directory -Path $tempDir | Out-Null
    $archivePath = Join-Path $tempDir $assetName
    $extractDir = Join-Path $tempDir 'extract'

    Write-Host "Downloading RTK v$rtkVersion from the official GitHub release..."
    Invoke-WebRequest -UseBasicParsing -Uri $assetUrl -OutFile $archivePath -Headers @{
        'User-Agent' = 'dail-extractor-rtk-pilot'
    }

    $downloadedHash = Get-Sha256Lower -Path $archivePath
    if ($downloadedHash -ne $archiveSha256) {
        throw "Archive checksum mismatch: expected $archiveSha256, got $downloadedHash."
    }

    Expand-Archive -LiteralPath $archivePath -DestinationPath $extractDir
    $extractedExe = Join-Path $extractDir 'rtk.exe'
    if (-not (Test-Path -LiteralPath $extractedExe -PathType Leaf)) {
        throw "The verified archive did not contain rtk.exe at its expected path."
    }

    $extractedHash = Get-Sha256Lower -Path $extractedExe
    if ($extractedHash -ne $executableSha256) {
        throw "Executable checksum mismatch: expected $executableSha256, got $extractedHash."
    }

    New-Item -ItemType Directory -Force -Path $installDir | Out-Null
    $stagedExe = Join-Path $installDir ("rtk.exe.pending-" + [Guid]::NewGuid().ToString('N'))
    Copy-Item -LiteralPath $extractedExe -Destination $stagedExe
    Move-Item -LiteralPath $stagedExe -Destination $rtkExe
    $stagedExe = $null

    Assert-RtkVersion -Path $rtkExe
    Write-Host "Installed and verified RTK v$rtkVersion at $rtkExe"
}
finally {
    if ($null -ne $stagedExe -and (Test-Path -LiteralPath $stagedExe)) {
        Remove-Item -LiteralPath $stagedExe -Force
    }
    if (Test-Path -LiteralPath $tempDir) {
        Remove-Item -LiteralPath $tempDir -Recurse -Force
    }
}
