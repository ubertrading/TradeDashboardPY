Phase 1: Google Cloud Console Setup (One-Time per Project)
Create Project:

Go to Google Cloud Console https://console.cloud.google.com/
while logged into the Google Account you want to back up to.

Create a new project (e.g., Rclone Backup).

Enable API:

Go to APIs & Services > Library.

Search for Google Drive API and click Enable.

Configure Consent & Test User:

Go to APIs & Services > OAuth consent screen (or Audience in the new layout).

Fill out standard fields (App Name, User support email, Developer contact email).

Keep the Publishing Status set to Testing (skips domain verification & privacy policy requirements).

Under Test users, click + ADD USERS and enter the target account's email address.

Generate Credentials:

Go to APIs & Services > Credentials.

Click + CREATE CREDENTIALS > OAuth client ID.

Select Desktop app as Application Type, name it Rclone, and click Create.

Copy the generated Client ID and Client Secret.

swaoalerts2
<CLIENT_ID_REDACTED>.apps.googleusercontent.com
<CLIENT_SECRET_REDACTED>

swapalerts1
<CLIENT_ID_REDACTED>.apps.googleusercontent.com
<CLIENT_SECRET_REDACTED>

Phase 2: Rclone Configuration (Per Target Account)
Launch Interactive Setup:

Run rclone config in PowerShell and choose n for a new remote.

Provide a unique remote name (e.g., gdrive_account2).

Choose option for Google Drive. 24

Pass Custom Credentials:

Paste your unique Client ID and Client Secret when prompted.

Select scope 1 (Full access).

Leave root_folder_id and service_account_file empty (press Enter).

OAuth Authentication:

Enter y to use auto config for browser authentication.

If using Chrome, copy the generated link ([http://127.0.0.1:53682/auth](http://127.0.0.1:53682/auth)?...) into an Incognito Window.

Log into the target Google Account, click Advanced > Go to Rclone (unsafe) on the warning prompt, and authorize access.

Decline Team Drive (n), accept the remote setup (y), and quit the config menu (q).

Phase 3: Script & Automation Setup
Command Execution:

Copy files: rclone copy "C:\Path\To\Source" "gdrive_account2:Backups/FolderName"  --progress --fast-list --transfers=4

Mirror files (deletes target files deleted locally): rclone sync "C:\Path\To\Source" "gdrive_account2:Backups/FolderName"

Schedule Script:

Save commands to a .ps1 script file.

Use Windows Task Scheduler (taskschd.msc) pointing to powershell.exe with arguments -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\Path\To\script.ps1"


------------------------
<#
.SYNOPSIS
    Automated Rclone backup script for Google Drive with logging and error handling.
.DESCRIPTION
    Performs an incremental backup of a specified source directory to a Google Drive remote.
    Includes lock-file management to prevent concurrent execution, automatic log rotation,
    and detailed transcript logging.
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $false)]
    [string]$SourceDir = "C:\Data\MyDirectory",

    [Parameter(Mandatory = $false)]
    [string]$RemoteName = "gdrive",

    [Parameter(Mandatory = $false)]
    [string]$RemoteDir = "Backups/MyDirectory",

    [Parameter(Mandatory = $false)]
    [string]$LogDir = "C:\Logs\RcloneBackups",

    [Parameter(Mandatory = $false)]
    [int]$LogRetentionDays = 30,

    [Parameter(Mandatory = $false)]
    [string]$RcloneExePath = "rclone.exe"
)

# ---------------------------------------------------------------------------
# 1. Environment & Path Initialization
# ---------------------------------------------------------------------------
$ErrorActionPreference = "Stop"
$TimeStamp = Get-Date -Format "yyyy-MM-dd_HHmmss"

# Ensure directories exist
if (-not (Test-Path -Path $SourceDir)) {
    Write-Error "Source directory does not exist: $SourceDir"
    exit 1
}

if (-not (Test-Path -Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

$LogFile = Join-Path -Path $LogDir -ChildPath "rclone_backup_$TimeStamp.log"
$LockFile = Join-Path -Path $LogDir -ChildPath "backup.lock"

# Start transcript logging for general script stdout/stderr
Start-Transcript -Path $LogFile -Append | Out-Null

Write-Host "========================================================="
Write-Host " Starting Backup Job: $TimeStamp"
Write-Host " Source:      $SourceDir"
Write-Host " Destination: $RemoteName`:$RemoteDir"
Write-Host "========================================================="

# ---------------------------------------------------------------------------
# 2. Prevent Concurrent Executions (Lock File Check)
# ---------------------------------------------------------------------------
if (Test-Path -Path $LockFile) {
    $LockInfo = Get-Content -Path $LockFile -ErrorAction SilentlyContinue
    Write-Warning "Lock file detected ($LockFile). Another instance may be running. Last run info: $LockInfo"
    Stop-Transcript | Out-Null
    exit 2
}

# Create Lock File with current process ID
"$TimeStamp - PID: $PID" | Out-File -FilePath $LockFile -Force

# ---------------------------------------------------------------------------
# 3. Clean Up Old Logs (Log Rotation)
# ---------------------------------------------------------------------------
try {
    Write-Host "Cleaning up log files older than $LogRetentionDays days..."
    Get-ChildItem -Path $LogDir -Filter "rclone_backup_*.log" | 
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$LogRetentionDays) } | 
        Remove-Item -Force -ErrorAction SilentlyContinue
} catch {
    Write-Warning "Failed to prune old log files: $_"
}

# ---------------------------------------------------------------------------
# 4. Execute Rclone Transfer
# ---------------------------------------------------------------------------
try {
    # Rclone execution arguments:
    # 'copy' avoids deleting cloud files if missing locally.
    # Replace 'copy' with 'sync' if you want an exact mirror (deletes remote files).
    $RcloneArgs = @(
        "copy",
        "$SourceDir",
        "$RemoteName`:$RemoteDir",
        "--fast-list",                   # Minimizes API calls to Google Drive
        "--transfers=4",                 # Parallel file transfers
        "--checkers=8",                  # Parallel file hash/timestamp checks
        "--retries=3",                   # Retry failed transfers
        "--low-level-retries=10",
        "--log-file=$LogFile",
        "--log-level=INFO",
        "--stats=1m"                     # Print status update every minute
    )

    Write-Host "Executing Rclone..."
    $Process = Start-Process -FilePath $RcloneExePath -ArgumentList $RcloneArgs -Wait -NoNewWindow -PassThru

    if ($Process.ExitCode -ne 0) {
        throw "Rclone process failed with exit code $($Process.ExitCode)."
    }

    Write-Host "Backup completed successfully!" -ForegroundColor Green

} catch {
    Write-Error "CRITICAL: Backup failed! Error details: $_"
    # Optional Hook: Send Windows notification or Webhook alert here
    $GlobalSuccess = $false
} finally {
    # ---------------------------------------------------------------------------
    # 5. Cleanup & Exit Handling
    # ---------------------------------------------------------------------------
    if (Test-Path -Path $LockFile) {
        Remove-Item -Path $LockFile -Force -ErrorAction SilentlyContinue
    }
    
    Write-Host "Job finished at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Stop-Transcript | Out-Null
}