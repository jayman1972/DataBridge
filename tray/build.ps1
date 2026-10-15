$ErrorActionPreference = "Stop"

$sources = @(
    (Join-Path $PSScriptRoot "DataBridgeTray.cs"),
    (Join-Path $PSScriptRoot "BridgeTrayLogic.cs"),
    (Join-Path $PSScriptRoot "TunnelRequestActivity.cs")
)
$outputDirectory = Join-Path $PSScriptRoot "bin"
$output = Join-Path $outputDirectory "DataBridgeTray.exe"
$compiler = Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\csc.exe"
if (-not (Test-Path -LiteralPath $compiler)) {
    $compiler = Join-Path $env:WINDIR "Microsoft.NET\Framework\v4.0.30319\csc.exe"
}
if (-not (Test-Path -LiteralPath $compiler)) {
    throw "The .NET Framework C# compiler is not installed on this Windows PC."
}
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
& $compiler /nologo /target:winexe /optimize+ /warn:4 /warnaserror+ "/out:$output" `
    /r:System.dll /r:System.Windows.Forms.dll /r:System.Drawing.dll `
    /r:System.Web.Extensions.dll $sources
if ($LASTEXITCODE -ne 0) { throw "DataBridge Tray compilation failed." }
Write-Host "Built $output"
