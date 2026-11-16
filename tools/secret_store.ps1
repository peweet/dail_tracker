<#
secret_store.ps1 - keep the secrets that unattended jobs need out of plain-text files.

Each secret is one file, %LOCALAPPDATA%\dail-secrets\<name>.dpapi, holding Windows DPAPI
output (ConvertFrom-SecureString with no key, so CurrentUser scope). Only this Windows user
on this machine can decrypt it.

What this stops: file grabbers and secret scanners (an infostealer, or a malicious npm install
running TruffleHog) that copy known credential files or search the disk for plain-text keys.
What it does not stop: code already running as this user that calls DPAPI itself or runs this
script. Bitwarden stays the escrow: a DPAPI file dies with this Windows profile.

Secrets in use (2026-11-16): restic_private, restic_sandbox (repository passwords) and
s3_r2, s3_hetzner_backup, s3_hetzner_siting (JSON {"AccessKeyId","SecretAccessKey"}, served to
rclone and the AWS CLI through credential_process in ~/.aws/config). See doc/CREDENTIAL_ESCROW.md.

Usage:
  secret_store.ps1 -Name restic_private -Get          # print the secret (for scripts)
  secret_store.ps1 -Name s3_r2 -AwsCredentialProcess  # print AWS credential_process JSON
  secret_store.ps1 -Name restic_private -Set          # read the value from stdin and store it
  secret_store.ps1 -List                              # stored names only
Dot-source it (`. tools\secret_store.ps1`) to call Get-DailSecret / Set-DailSecret directly.
#>
param(
    [string]$Name,
    [switch]$Get,
    [switch]$Set,
    [switch]$AwsCredentialProcess,
    [switch]$List
)

$script:DailSecretDir = Join-Path $env:LOCALAPPDATA 'dail-secrets'

function Get-DailSecretPath([string]$SecretName) {
    if ($SecretName -notmatch '^[a-z0-9_]+$') { throw "Bad secret name: '$SecretName'" }
    Join-Path $script:DailSecretDir "$SecretName.dpapi"
}

function Get-DailSecret([string]$SecretName) {
    $path = Get-DailSecretPath $SecretName
    if (-not (Test-Path $path)) {
        throw "Secret '$SecretName' not found in $script:DailSecretDir - see doc/CREDENTIAL_ESCROW.md"
    }
    $secure = (Get-Content -Raw -Path $path).Trim() | ConvertTo-SecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try { [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

function Set-DailSecret([string]$SecretName, [string]$Value) {
    if ([string]::IsNullOrEmpty($Value)) { throw "Refusing to store an empty secret for '$SecretName'" }
    New-Item -ItemType Directory -Force -Path $script:DailSecretDir | Out-Null
    $path = Get-DailSecretPath $SecretName
    $blob = ConvertTo-SecureString -String $Value -AsPlainText -Force | ConvertFrom-SecureString
    # Temp file then rename: a full disk must not leave a truncated secret behind.
    $tmp = "$path.tmp"
    Set-Content -Path $tmp -Value $blob -Encoding ASCII
    Move-Item -Force -Path $tmp -Destination $path
    if ((Get-DailSecret $SecretName) -cne $Value) { throw "Round trip failed for '$SecretName'" }
}

function Get-DailAwsCredentialJson([string]$SecretName) {
    $pair = Get-DailSecret $SecretName | ConvertFrom-Json
    if (-not $pair.AccessKeyId -or -not $pair.SecretAccessKey) {
        throw "Secret '$SecretName' is not an AccessKeyId/SecretAccessKey pair"
    }
    [ordered]@{ Version = 1; AccessKeyId = $pair.AccessKeyId; SecretAccessKey = $pair.SecretAccessKey } |
        ConvertTo-Json -Compress
}

# Dot-sourced with no switch: define the functions and stop.
if (-not ($Get -or $Set -or $AwsCredentialProcess -or $List)) { return }

try {
    if ($List) {
        if (Test-Path $script:DailSecretDir) {
            Get-ChildItem -Path $script:DailSecretDir -Filter '*.dpapi' | ForEach-Object { $_.BaseName }
        }
    } elseif ($Get) {
        [Console]::Out.Write((Get-DailSecret $Name))
    } elseif ($AwsCredentialProcess) {
        [Console]::Out.Write((Get-DailAwsCredentialJson $Name))
    } elseif ($Set) {
        Set-DailSecret $Name ([Console]::In.ReadToEnd().TrimEnd("`r", "`n"))
        Write-Host "stored $Name"
    }
    exit 0
} catch {
    [Console]::Error.WriteLine("secret_store: $($_.Exception.Message)")
    exit 1
}
