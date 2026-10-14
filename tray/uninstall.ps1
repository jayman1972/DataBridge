$ErrorActionPreference = "Stop"
$taskName = "EHP DataBridge Tray"
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($task) {
    $user = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if ($task.Principal.UserId -notin @($user, $userSid)) {
        throw "The task belongs to $($task.Principal.UserId), not $user. Refusing to remove it."
    }
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed '$taskName' auto-start task."
} else {
    Write-Host "No '$taskName' auto-start task was installed."
}
Write-Host "If the tray app is running, use its Exit menu item to stop it."
