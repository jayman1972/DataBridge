$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "build.ps1")

$exe = Join-Path $PSScriptRoot "bin\DataBridgeTray.exe"
$testProcess = Start-Process -FilePath $exe -ArgumentList "--self-test" `
    -WindowStyle Hidden -Wait -PassThru
if ($testProcess.ExitCode -ne 0) {
    throw "DataBridge Tray self-test failed with exit code $($testProcess.ExitCode)."
}

$scripts = @(
    (Join-Path $PSScriptRoot "build.ps1"),
    (Join-Path $PSScriptRoot "install.ps1"),
    (Join-Path $PSScriptRoot "uninstall.ps1"),
    (Join-Path $PSScriptRoot "test.ps1"),
    (Join-Path (Split-Path $PSScriptRoot -Parent) "start-tunnel-ngrok.ps1"),
    (Join-Path (Split-Path $PSScriptRoot -Parent) "scripts\check_psc_nav.ps1")
)
foreach ($script in $scripts) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile(
        $script, [ref]$tokens, [ref]$errors
    ) | Out-Null
    if ($errors.Count -gt 0) {
        throw "PowerShell parse failed for $script`: $($errors[0].Message)"
    }
}
Write-Host "DataBridge Tray build, self-test, and PowerShell parse checks passed."
