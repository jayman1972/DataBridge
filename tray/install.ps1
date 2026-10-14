[CmdletBinding()]
param([switch]$StartNow, [switch]$TunnelAccessPolicyVerified)

$ErrorActionPreference = "Stop"
if (-not $TunnelAccessPolicyVerified) {
    throw "Unattended auto-start requires verified tunnel access control. Secure the public DataBridge routes, then rerun with -TunnelAccessPolicyVerified."
}
& (Join-Path $PSScriptRoot "build.ps1")
if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) { throw "DataBridge Tray build failed." }

$exe = Join-Path $PSScriptRoot "bin\DataBridgeTray.exe"
$taskName = "EHP DataBridge Tray"
$user = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing -and $existing.Principal.UserId -notin @($user, $userSid)) {
    throw "A task named '$taskName' already belongs to $($existing.Principal.UserId). Refusing to replace it."
}
$action = New-ScheduledTaskAction -Execute $exe
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings `
    -Description "Starts the local PSC/Bloomberg DataBridge and ngrok in the signed-in user's tray." `
    -Force | Out-Null

Write-Host "Installed '$taskName' for $user. It starts at this user's next sign-in."
Write-Host "Close the two manual DataBridge/ngrok windows before starting the tray app."
if ($StartNow) {
    $bridgeInUse = $false
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $bridgeInUse = $client.ConnectAsync("127.0.0.1", 5000).Wait(1000) -and $client.Connected
        } catch [System.AggregateException] {
            $bridgeInUse = $false
        }
    } finally {
        if ($client) { $client.Dispose() }
    }
    if ($bridgeInUse) {
        Write-Warning "Port 5000 is already in use. The task is installed but was not started."
    } else {
        Start-ScheduledTask -TaskName $taskName
    }
}
