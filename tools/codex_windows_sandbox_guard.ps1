<#
.SYNOPSIS
Safely quarantines a malformed Codex Windows deny-read ACL state file.

.DESCRIPTION
The Codex Windows sandbox setup runs before child-process startup. If its
deny_read_acl_state.json is malformed, even trivial commands can fail with
"apply deny-read ACLs". This guard retries validation to avoid racing a write,
then moves (never deletes) the malformed state and its matching setup error to
timestamped backups. Codex remains responsible for regenerating its own state.

The script deliberately does not change ACLs, kill processes, inspect sandbox
secrets, or manufacture a replacement state file.
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$StatePath,
    [string]$SetupErrorPath,
    [string]$LogPath,
    [ValidateRange(1, 10)]
    [int]$RetryCount = 3,
    [ValidateRange(0, 5000)]
    [int]$RetryDelayMilliseconds = 300
)

$ErrorActionPreference = 'Stop'

if (-not $StatePath) {
    $StatePath = Join-Path $env:USERPROFILE '.codex\.sandbox\deny_read_acl_state.json'
}
if (-not $SetupErrorPath) {
    $SetupErrorPath = Join-Path $env:USERPROFILE '.codex\.sandbox\setup_error.json'
}
if (-not $LogPath) {
    $LogPath = Join-Path $env:USERPROFILE '.codex\.sandbox\auto-repair.log'
}

function Test-CodexAclState {
    param([Parameter(Mandatory = $true)][string]$LiteralPath)

    if (-not (Test-Path -LiteralPath $LiteralPath -PathType Leaf)) {
        return [pscustomobject]@{ Valid = $true; Exists = $false; Reason = 'missing' }
    }

    try {
        $bytes = [System.IO.File]::ReadAllBytes($LiteralPath)
        if ($bytes.Length -eq 0) {
            return [pscustomobject]@{ Valid = $false; Exists = $true; Reason = 'empty' }
        }
        if ([Array]::IndexOf($bytes, [byte]0) -ge 0) {
            return [pscustomobject]@{ Valid = $false; Exists = $true; Reason = 'contains-nul' }
        }

        $strictUtf8 = New-Object System.Text.UTF8Encoding($false, $true)
        $text = $strictUtf8.GetString($bytes)
        if ([string]::IsNullOrWhiteSpace($text)) {
            return [pscustomobject]@{ Valid = $false; Exists = $true; Reason = 'blank' }
        }
        $parsed = $text | ConvertFrom-Json -ErrorAction Stop
        if ($null -eq $parsed) {
            return [pscustomobject]@{ Valid = $false; Exists = $true; Reason = 'json-null' }
        }
        return [pscustomobject]@{ Valid = $true; Exists = $true; Reason = 'valid-json' }
    }
    catch {
        return [pscustomobject]@{ Valid = $false; Exists = $true; Reason = 'invalid-json-or-utf8' }
    }
}

function Write-GuardLog {
    param([Parameter(Mandatory = $true)][string]$Message)

    $parent = Split-Path -Parent $LogPath
    if ($parent) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $timestamp = [DateTime]::UtcNow.ToString('o')
    Add-Content -LiteralPath $LogPath -Value "$timestamp $Message" -Encoding UTF8
}

function Move-ToTimestampedBackup {
    param([Parameter(Mandatory = $true)][string]$LiteralPath)

    $directory = Split-Path -Parent $LiteralPath
    $filename = Split-Path -Leaf $LiteralPath
    $stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
    $destination = Join-Path $directory "$filename.corrupt-auto-$stamp.bak"
    Move-Item -LiteralPath $LiteralPath -Destination $destination -ErrorAction Stop
    return $destination
}

$probe = $null
for ($attempt = 1; $attempt -le $RetryCount; $attempt++) {
    $probe = Test-CodexAclState -LiteralPath $StatePath
    if ($probe.Valid) {
        Write-Output "Codex sandbox ACL state is $($probe.Reason); no repair needed."
        exit 0
    }
    if ($attempt -lt $RetryCount -and $RetryDelayMilliseconds -gt 0) {
        Start-Sleep -Milliseconds $RetryDelayMilliseconds
    }
}

$mutex = [System.Threading.Mutex]::new($false, 'Local\DailExtractorCodexSandboxGuard')
$acquired = $false
try {
    $acquired = $mutex.WaitOne(5000)
    if (-not $acquired) {
        throw 'Timed out waiting for another Codex sandbox guard instance.'
    }

    # A concurrent Codex setup or guard may have repaired the file while this
    # instance waited. Revalidate under the mutex before changing anything.
    $probe = Test-CodexAclState -LiteralPath $StatePath
    if ($probe.Valid) {
        Write-Output "Codex sandbox ACL state became $($probe.Reason); no repair needed."
        exit 0
    }

    if (-not $PSCmdlet.ShouldProcess($StatePath, "quarantine malformed Codex ACL state ($($probe.Reason))")) {
        Write-Output "Would quarantine malformed Codex sandbox ACL state ($($probe.Reason))."
        exit 0
    }

    $stateBackup = Move-ToTimestampedBackup -LiteralPath $StatePath
    $errorBackup = $null
    if (Test-Path -LiteralPath $SetupErrorPath -PathType Leaf) {
        try {
            $setupError = Get-Content -LiteralPath $SetupErrorPath -Raw -ErrorAction Stop
            if ($setupError -match 'parse deny-read ACL state') {
                $errorBackup = Move-ToTimestampedBackup -LiteralPath $SetupErrorPath
            }
        }
        catch {
            # The malformed state is the launch blocker. Preserve any setup
            # error we cannot positively associate instead of broadening repair.
            $errorBackup = $null
        }
    }

    $message = "quarantined malformed deny-read ACL state reason=$($probe.Reason) backup=$stateBackup"
    if ($errorBackup) {
        $message += " linked_setup_error_backup=$errorBackup"
    }
    Write-GuardLog -Message $message
    Write-Output 'Repaired Codex sandbox startup state; Codex will regenerate the missing state file.'
    Write-Output "Preserved malformed state at: $stateBackup"
    if ($errorBackup) {
        Write-Output "Preserved linked setup error at: $errorBackup"
    }
    exit 0
}
catch {
    try {
        Write-GuardLog -Message "repair failed error=$($_.Exception.GetType().Name)"
    }
    catch {
        # Retain the original repair failure if even diagnostic logging fails.
    }
    throw
}
finally {
    if ($acquired) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
