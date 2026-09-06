<#
.SYNOPSIS
    Sync this project into the FreeBSD VM and build it there.

.DESCRIPTION
    Source of truth is ALWAYS this Windows directory. The copy inside the VM
    (~/A2) is a build/run mirror -- never edit files there, they get
    overwritten on the next deploy.

    Transfer is one tar + one scp + one ssh, so a deploy costs a single SSH
    handshake instead of one per file.

.EXAMPLE
    .\deploy.ps1                      # sync, then run 'make' in the VM
    .\deploy.ps1 -NoBuild             # sync only
    .\deploy.ps1 -Clean               # wipe ~/A2 first, then sync and build
    .\deploy.ps1 -Exec "./server/run-server 127.0.0.1 5000"
#>
[CmdletBinding()]
param(
    # Skip the build step; just copy the files across.
    [switch]$NoBuild,

    # Delete the remote directory before extracting, so files deleted on
    # Windows also disappear in the VM. Off by default: it is destructive.
    [switch]$Clean,

    # Run this command in the VM (inside ~/A2) after a successful build.
    [string]$Exec,

    [string]$VMHost   = 'bsd',
    [string]$RemoteDir = 'A2'
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot

# Everything that should exist inside the VM. Missing entries are skipped, so
# this list can name things that do not exist yet.
$payload = @('src', 'server', 'client', 'bonus', 'Makefile', 'experiment.py', 'README.md')

$present = @()
foreach ($item in $payload) {
    if (Test-Path (Join-Path $root $item)) { $present += $item }
}

if ($present.Count -eq 0) {
    throw "Nothing to deploy: none of [$($payload -join ', ')] exist in $root"
}

Write-Host "Deploying: $($present -join ', ')" -ForegroundColor Cyan

# --- 1. Pack -------------------------------------------------------------
# bsdtar ships with Windows 10+. Packing preserves the directory layout and
# avoids scp -r's habit of nesting src/ inside an existing src/.
$tarball = Join-Path $env:TEMP 'a2-deploy.tar'
if (Test-Path $tarball) { Remove-Item $tarball -Force }

& tar -cf $tarball -C $root @present
if ($LASTEXITCODE -ne 0) { throw "tar failed (exit $LASTEXITCODE)" }

# --- 2. Ship -------------------------------------------------------------
& scp -q $tarball "${VMHost}:/tmp/a2-deploy.tar"
if ($LASTEXITCODE -ne 0) {
    throw "scp to '$VMHost' failed (exit $LASTEXITCODE). Is the VM running and the SSH key installed?"
}

# --- 3. Unpack in the VM -------------------------------------------------
# Built as a single line: a stray CR from a Windows editor inside a multi-line
# remote command would break /bin/sh in the VM.
#
# The sed pass strips CRLF from the launcher scripts (a '\r' after '#!/bin/sh'
# makes FreeBSD report a bogus "bad interpreter" error), and chmod restores the
# executable bit, which NTFS does not carry across.
$wipe = ''
if ($Clean) { $wipe = "rm -rf `$HOME/$RemoteDir; " }

$fix = "for f in server/run-server client/run-trader client/run-market-data bonus/setup-freebsd.sh; do if [ -f `"`$f`" ]; then sed -i '' -e 's/`r`$//' `"`$f`"; chmod +x `"`$f`"; fi; done"

# tar preserves each file's mtime from Windows. If the VM's clock has
# drifted ahead of the host's (it has -- seen ~5.5h fast), a freshly synced
# source file can carry an mtime that looks OLDER than an already-built
# binary's VM-clock timestamp, so `make` silently thinks the binary is
# still up to date and skips rebuilding it. Touching every just-extracted
# file to the VM's own current clock reading avoids comparing timestamps
# across two different clocks: extracted files are always "now" on
# whichever clock `make` itself will use.
$touchList = ($present -join ' ')
$touch = "find $touchList -type f -exec touch {} +"

$unpack = "$wipe" + "mkdir -p `$HOME/$RemoteDir && tar -xf /tmp/a2-deploy.tar -C `$HOME/$RemoteDir && rm -f /tmp/a2-deploy.tar && cd `$HOME/$RemoteDir && $fix && $touch"

& ssh $VMHost $unpack
if ($LASTEXITCODE -ne 0) { throw "Remote unpack failed (exit $LASTEXITCODE)" }

Write-Host "Synced to ${VMHost}:~/$RemoteDir" -ForegroundColor Green

# --- 4. Build ------------------------------------------------------------
if (-not $NoBuild) {
    if (Test-Path (Join-Path $root 'Makefile')) {
        Write-Host "--- make ---" -ForegroundColor Cyan
        & ssh $VMHost "cd `$HOME/$RemoteDir && make"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "BUILD FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
            exit $LASTEXITCODE
        }
        Write-Host "Build OK" -ForegroundColor Green
    }
    else {
        Write-Host "No Makefile yet; skipping build." -ForegroundColor Yellow
    }
}

# --- 5. Optional run -----------------------------------------------------
if ($Exec) {
    Write-Host "--- $Exec ---" -ForegroundColor Cyan
    & ssh $VMHost "cd `$HOME/$RemoteDir && $Exec"
    exit $LASTEXITCODE
}
