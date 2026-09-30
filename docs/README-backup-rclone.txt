================================================================================
 Rclone Google Drive Backup — Setup Guide (OAuth Method)
================================================================================

Authentication method: OAuth with your own Client ID/Secret
Auth expires: NEVER (as long as app is "In production" and token is used)
Files on server: C:\rclone\

Files:
    rclone.exe          - The rclone binary
    rclone.conf         - Remote configuration (client_id, client_secret, token)
    backup_TD.ps1       - The backup script
    log\                - Log directory (rclone_execution_YYYY-MM-DD_HHmmss.log)


================================================================================
 Why OAuth, Not Service Account
================================================================================

We attempted to use a Service Account (JSON key file) but it fails with:

    ERROR: googleapi: Error 403: Service Accounts do not have storage quota.
    Leverage shared drives or use OAuth delegation instead., storageQuotaExceeded

Service accounts have 0 MB quota on personal @gmail.com accounts. Any NEW file
upload is rejected. Only existing files can be checked/compared. This makes
service accounts useless for backup to personal Google Drive.

Service accounts only work with Google Workspace Shared Drives (corporate accounts).

OAuth authenticates as the actual Gmail user and uses their full storage quota.


================================================================================
 Why Your Own Client ID (Not Rclone's Default)
================================================================================

Rclone's shared default client_id is being retired during 2026 and will stop
working. You must create your own.

Additionally, with your own client_id you control the OAuth app publishing
status, which determines token lifetime:

    Testing mode    -> Refresh tokens expire every 7 days (bad for automation)
    In production   -> Refresh tokens never expire (correct for automation)


================================================================================
 Phase 1: Google Cloud Console Setup (One-Time per Project)
================================================================================

1. Create Project:

   Go to https://console.cloud.google.com/
   Log in with ANY Google account (does not need to be the backup target).
   Create a new project (e.g., "Rclone Backup").

2. Enable the Google Drive API:

   Go to APIs & Services > Library.
https://console.cloud.google.com/apis/library?
   Search for "Google Drive API" and click Enable.

3. Configure OAuth Consent Screen:

   Go to APIs & Services > Credentials > "CONFIGURE CONSENT SCREEN"
(actually under "Branding")
https://console.cloud.google.com/auth/branding?
   (or in the new UI: Google Auth Platform > Get Started)

   Fill in:
       App name:             Rclone Backup
       User support email:   (your email)
       Developer contact:    (your email)

   *** IMPORTANT: Authorized Domains / Branding ***

   Google now requires "Application home page" and "Application privacy policy
   link" before allowing you to publish. These fields are marked mandatory (*).
   Enter your website URL for both (e.g., https://www.quantflash.com).
   Add the domain (e.g., quantflash.com) to "Authorized domains".

   *** DO NOT submit for verification. DO NOT click "Prepare for verification". ***

   This is for personal/internal use only. You just need the "Publish App" step
   below. Verification is only required for apps distributed to 100+ external users.
https://console.cloud.google.com/auth/audience?

4. Add Scopes:

   Go to Data Access (or Scopes section).
   Click "Add or remove scopes".
   Add: https://www.googleapis.com/auth/drive
   Click Update, then Save.

5. Add Yourself as a Test User:

   Go to Audience > Test users > "+ Add users".
   Add the email address of the Google account you want to back up to.
   Save.

6. Publish to Production:

   In the Audience tab, click "PUBLISH APP" and click Confirm.

   The status will change to "In production" (Verification: Unverified).
   This is correct. Tokens will now NEVER expire due to the 7-day testing limit.

   *** DO NOT click "Submit for verification" or "Prepare for verification". ***

7. Create OAuth Client Credentials:

   Go to APIs & Services > Credentials (or Google Auth Platform > Clients).
   Click "+ CREATE CLIENT" (or Create Credentials > OAuth client ID).
   Application type: Desktop app
   Name: Rclone
   Click Create.

   Copy and save:
       Client ID
       Client Secret


================================================================================
 Current Credentials Reference
================================================================================

swapalerts1 (backup target: swapalerts1@gmail.com):
    Client ID:     51370669947-lb2o20ved5plhc7lsq5vqo9uvm60uc26.apps.googleusercontent.com
    Client Secret: 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

swapalerts2 (backup target: swapalerts2@gmail.com):
882597329369-eik90gmgl9ebshph9vhb4gba3h4ls0p7.apps.googleusercontent.com
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


================================================================================
 Phase 2: Rclone Configuration (Per Target Account)
================================================================================

1. Edit C:\rclone\rclone.conf to contain ONLY these lines:

   [swapalerts1]
   type = drive
   scope = drive
   client_id = YOUR_CLIENT_ID.apps.googleusercontent.com
   client_secret = YOUR_CLIENT_SECRET

   *** Make sure there is NO service_account_file line. ***
   *** Make sure there is NO root_folder_id line (use Drive root). ***
   *** If either of those lines exist, rclone will skip browser auth. ***

2. Run browser authentication:

   rclone config reconnect swapalerts1:

   When prompted "Use web browser to automatically authenticate?":
       - Type y and press Enter
       - Complete Google login in the browser
       - On the "Google hasn't verified this app" screen:
         Click "Advanced" > "Go to Rclone Backup (unsafe)" > "Continue / Allow"
       - Configure as Shared Drive (Team Drive)? -> n

   Rclone will write the OAuth token into rclone.conf automatically.

3. Verify it works:

   rclone lsd swapalerts1:
   (Should list the top-level folders of your Google Drive)

   rclone sync "E:\SWAP\trade_dashboard" "swapalerts1:Backups/trade_dashboard" --progress --fast-list --transfers=4
   (Should transfer files successfully)

4. Verify final rclone.conf:

   rclone config dump

   Should show: type, scope, client_id, client_secret, token
   Should NOT show: service_account_file


================================================================================
 Phase 3: Script & Paths
================================================================================

Script location (server):  C:\rclone\backup_TD.ps1
Config location (server):  C:\rclone\rclone.conf
Log directory (server):    C:\rclone\log\

Script default parameters:
    SourceDir  = E:\SWAP\trade_dashboard
    RemoteName = swapalerts1
    RemoteDir  = trade_dashboard         <- relative to Drive root
    LogDir     = C:\rclone\log
    LogRetentionDays = 30

NOTE: RemoteDir is relative to the Drive ROOT (no root_folder_id set).
The full remote path is: swapalerts1:trade_dashboard
Which maps to the "trade_dashboard" folder inside the "Backups" folder that
you shared — wait, actually without root_folder_id it maps directly to
"trade_dashboard" at the root of the Drive.

Confirm the exact path with:
    rclone lsd swapalerts1:

Manual sync command (equivalent to what the script runs):
    rclone sync "E:\SWAP\trade_dashboard" "swapalerts1:trade_dashboard" --progress --fast-list --transfers=4

Run the script manually to test:
    powershell.exe -ExecutionPolicy Bypass -File "C:\rclone\backup_TD.ps1"

Schedule (Windows Task Scheduler):
    Program:   powershell.exe
    Arguments: -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\rclone\backup_TD.ps1"
    Schedule:  Hourly (currently configured in Task Scheduler as "rclone TD to swapalerts1")


================================================================================
 Troubleshooting
================================================================================

"couldn't fetch token: invalid_grant: maybe token expired?":
    Cause: Either the OAuth app is still in "Testing" mode (7-day expiry),
           or the token was manually revoked.
    Fix:   1. Confirm app is "In production" in Google Cloud Console.
           2. Re-authenticate: rclone config reconnect swapalerts1:

"Service Accounts do not have storage quota. storageQuotaExceeded":
    Cause: The rclone.conf still has a service_account_file line.
    Fix:   Open C:\rclone\rclone.conf in Notepad and delete the
           service_account_file line and root_folder_id line.
           Then run: rclone config reconnect swapalerts1:

rclone config reconnect doesn't ask to open browser:
    Cause: service_account_file is still present in rclone.conf — rclone
           uses the JSON key file instead of OAuth when that line exists.
    Fix:   Remove service_account_file (and root_folder_id) from rclone.conf
           manually, then retry: rclone config reconnect swapalerts1:

"directory not found" when listing remote:
    Cause: root_folder_id is set and the folder ID is wrong, or the folder
           was not shared with the account.
    Fix:   Remove root_folder_id from rclone.conf. The root will then be the
           actual Google Drive root of the authenticated account.

Script runs but no files transferred (exit 0, 0 bytes transferred):
    Cause: All files already exist in Drive and are unchanged — this is normal.
    Verify with: rclone lsd swapalerts1:trade_dashboard

Script uses wrong remote path (e.g., Backups/Backups/trade_dashboard):
    Cause: RemoteDir in backup_TD.ps1 was "Backups/trade_dashboard" while
           root_folder_id already pointed to the Backups folder.
    Fix:   RemoteDir should be "trade_dashboard" (no Backups prefix) when
           root_folder_id is not set, or just "trade_dashboard" from Drive root.

Token expires after ~6 months of no use:
    This is normal Google behaviour even in Production mode.
    Fix:   Re-authenticate: rclone config reconnect swapalerts1:
    Prevention: The hourly scheduled backup ensures the token is used regularly.


================================================================================
 Backup Script Template (backup_TD.ps1)
================================================================================

<#
.SYNOPSIS
    Automated Rclone backup script for Google Drive with logging and error handling.
.DESCRIPTION
    Performs an incremental sync of a specified source directory to a Google Drive remote.
    Uses OAuth authentication via rclone.conf (no service account).
    Includes lock-file management to prevent concurrent execution and automatic log rotation.
#>

[CmdletBinding()]
param (
    [Parameter(Mandatory = $false)]
    [string]$SourceDir = "E:\SWAP\trade_dashboard",

    [Parameter(Mandatory = $false)]
    [string]$RemoteName = "swapalerts1",

    [Parameter(Mandatory = $false)]
    [string]$RemoteDir = "trade_dashboard",

    [Parameter(Mandatory = $false)]
    [string]$LogDir = "C:\rclone\log",

    [Parameter(Mandatory = $false)]
    [int]$LogRetentionDays = 30,

    [Parameter(Mandatory = $false)]
    [string]$RcloneExePath = "C:\rclone\rclone.exe"
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

$LockFile = Join-Path -Path $LogDir -ChildPath "backup.lock"

# ---------------------------------------------------------------------------
# 2. Prevent Concurrent Executions (Lock File Check)
# ---------------------------------------------------------------------------
if (Test-Path -Path $LockFile) {
    $LockInfo = Get-Content -Path $LockFile -ErrorAction SilentlyContinue
    Write-Warning "Lock file detected ($LockFile). Another instance may be running. Last run info: $LockInfo"
    exit 2
}

# Create Lock File with current process ID
"$TimeStamp - PID: $PID" | Out-File -FilePath $LockFile -Force

# ---------------------------------------------------------------------------
# 3. Clean Up Old Logs (Log Rotation)
# ---------------------------------------------------------------------------
try {
    Get-ChildItem -Path $LogDir -Filter "rclone_execution_*.log" |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$LogRetentionDays) } |
        Remove-Item -Force -ErrorAction SilentlyContinue
} catch {
    Write-Warning "Failed to prune old log files: $_"
}

# ---------------------------------------------------------------------------
# 4. Execute Rclone Transfer
# ---------------------------------------------------------------------------
$ExitCode = 0

try {
    $RcloneLogFile = Join-Path -Path $LogDir -ChildPath "rclone_execution_$TimeStamp.log"
    $ConfigFile = "C:\rclone\rclone.conf"

    $RcloneArgs = @(
        "sync",
        "$SourceDir",
        "$RemoteName`:$RemoteDir",
        "--config=$ConfigFile",
        "--fast-list",
        "--transfers=4",
        "--checkers=8",
        "--retries=3",
        "--low-level-retries=10",
        "--log-file=$RcloneLogFile",
        "--log-level=INFO",
        "--stats=1m"
    )

    $Executable = if (Test-Path "C:\rclone\rclone.exe") { "C:\rclone\rclone.exe" } else { $RcloneExePath }

    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = $Executable
    $pinfo.Arguments = ($RcloneArgs -join " ")
    $pinfo.UseShellExecute = $false
    $pinfo.CreateNoWindow = $true

    $Process = [System.Diagnostics.Process]::Start($pinfo)
    $Process.WaitForExit()

    if ($Process.ExitCode -ne 0) {
        throw "Rclone process failed with exit code $($Process.ExitCode)."
    }

} catch {
    Write-Error "CRITICAL: Backup failed! Error details: $_"
    $ExitCode = 1
} finally {
    # ---------------------------------------------------------------------------
    # 5. Cleanup & Exit Handling
    # ---------------------------------------------------------------------------
    if (Test-Path -Path $LockFile) {
        Remove-Item -Path $LockFile -Force -ErrorAction SilentlyContinue
    }
}

exit $ExitCode