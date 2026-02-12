# PowerShell script to start ngrok and update Supabase secrets
# Run from projects\DataBridge
# Supports: -NgrokPath "path\to\ngrok.exe" or "ngrok" (from PATH)

param([string]$NgrokPath = "")

$ErrorActionPreference = "Continue"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Starting ngrok tunnel..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$MARKET_DASHBOARD_PROJECT_REF = "aphjduxfgsrqswonmgyb"
$WEALTH_SCOPE_PROJECT_REF = "gndrtpvvldqalpbvvgrr"
$PORT = 5000

# Paths: script is in projects/DataBridge
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECTS_DIR = Split-Path $SCRIPT_DIR -Parent
$MARKET_DASHBOARD_DIR = Join-Path $PROJECTS_DIR "market-dashboard"
$WEALTH_SCOPE_DIR = Join-Path $PROJECTS_DIR "wealth-scope-ui"

Write-Host "Checking for existing ngrok tunnel on port $PORT..." -ForegroundColor Yellow
$existingNgrokUrl = $null

try {
    $tunnelsResponse = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 2 -ErrorAction Stop
    if ($tunnelsResponse.tunnels -and $tunnelsResponse.tunnels.Count -gt 0) {
        $candidateTunnels = $tunnelsResponse.tunnels | Where-Object { $_.config.addr -like "*:$PORT" }
        if ($candidateTunnels) {
            $httpsTunnel = $candidateTunnels | Where-Object { $_.public_url -like "https://*" } | Select-Object -First 1
            $existingNgrokUrl = if ($httpsTunnel) { $httpsTunnel.public_url } else { ($candidateTunnels | Select-Object -First 1).public_url }
        }
    }
} catch { }

if ($existingNgrokUrl) {
    $existingTunnelHealthy = $false
    try {
        $healthUrl = ($existingNgrokUrl.TrimEnd('/')) + "/health"
        $healthResp = Invoke-WebRequest -Uri $healthUrl -Method Get -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        if ($healthResp.StatusCode -eq 200) { $existingTunnelHealthy = $true }
    } catch { }

    if ($existingTunnelHealthy) {
        Write-Host "Ngrok tunnel already running: $existingNgrokUrl" -ForegroundColor Green
        exit 0
    }
}

function Update-SupabaseSecret {
    param([string]$ProjectDir, [string]$ProjectName, [string]$ProjectRef, [string]$Url)
    if (-not (Test-Path $ProjectDir)) {
        Write-Host "WARNING: $ProjectName not found at $ProjectDir" -ForegroundColor Yellow
        return $false
    }
    Push-Location $ProjectDir
    try {
        Write-Host "Updating Supabase secret for $ProjectName..." -ForegroundColor Yellow
        $linkResult = supabase link --project-ref $ProjectRef 2>&1
        supabase secrets set "DATA_BRIDGE_URL=$Url" 2>&1
        supabase secrets set "BLOOMBERG_BRIDGE_URL=$Url" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  [OK] $ProjectName" -ForegroundColor Green
            return $true
        }
        return $false
    } catch {
        Write-Host "  Error: $_" -ForegroundColor Red
        return $false
    } finally { Pop-Location }
}

Write-Host "Starting ngrok on port $PORT..." -ForegroundColor Yellow
# Prefer passed path, then local ngrok.exe, then ngrok from PATH
$ngrokExe = $NgrokPath
if (-not $ngrokExe) {
    $localNgrok = Join-Path $SCRIPT_DIR "ngrok.exe"
    if (Test-Path $localNgrok) {
        $ngrokExe = $localNgrok
    } else {
        $ngrokExe = "ngrok"
    }
}
$ngrokProcess = Start-Process -FilePath $ngrokExe -ArgumentList "http", $PORT -NoNewWindow -PassThru -RedirectStandardOutput "ngrok_output.txt" -RedirectStandardError "ngrok_error.txt"
Start-Sleep -Seconds 3

$ngrokUrl = $null
for ($i = 0; $i -lt 10; $i++) {
    try {
        $api = Invoke-RestMethod -Uri "http://localhost:4040/api/tunnels" -ErrorAction SilentlyContinue
        if ($api.tunnels -and $api.tunnels.Count -gt 0) {
            $ngrokUrl = ($api.tunnels | Where-Object { $_.public_url -like "https://*" } | Select-Object -First 1).public_url
            if (-not $ngrokUrl) { $ngrokUrl = $api.tunnels[0].public_url }
            break
        }
    } catch { }
    Start-Sleep -Seconds 1
}

if ($ngrokUrl) {
    Write-Host "Ngrok tunnel: $ngrokUrl" -ForegroundColor Green
    $currentUser = $env:USERNAME
    $isAdmin = @("jayma", "jmann") -contains $currentUser.ToLower()
    if ($env:ADMIN_TUNNEL -eq "0") { $isAdmin = $false }
    if ($env:ADMIN_TUNNEL -eq "1") { $isAdmin = $true }

    if ($env:SKIP_SUPABASE_UPDATE -eq "1" -or -not $isAdmin) {
        Write-Host "Skipping Supabase update (non-admin or SKIP_SUPABASE_UPDATE=1)" -ForegroundColor Yellow
    } else {
        Update-SupabaseSecret -ProjectDir $MARKET_DASHBOARD_DIR -ProjectName "market-dashboard" -ProjectRef $MARKET_DASHBOARD_PROJECT_REF -Url $ngrokUrl | Out-Null
        if (Test-Path $WEALTH_SCOPE_DIR) {
            Update-SupabaseSecret -ProjectDir $WEALTH_SCOPE_DIR -ProjectName "wealth-scope" -ProjectRef $WEALTH_SCOPE_PROJECT_REF -Url $ngrokUrl | Out-Null
        }
    }
    Write-Host ""
    Write-Host "Tunnel health monitor: checking every 30 seconds (through public URL)" -ForegroundColor Cyan
    Write-Host "Keep this window open. Press Ctrl+C to stop ngrok." -ForegroundColor Cyan
    Write-Host ""

    # Health check through the tunnel (more reliable - uses actual public URL)
    $healthUrl = ($ngrokUrl.TrimEnd('/')) + "/health"
    $checkInterval = 30
    $consecutiveFailures = 0

    # Initial health check after 5 seconds
    Start-Sleep -Seconds 5
    if (-not $ngrokProcess.HasExited) {
        try {
            $headers = @{}
            if ($ngrokUrl -match "ngrok") { $headers["ngrok-skip-browser-warning"] = "1" }
            $resp = Invoke-WebRequest -Uri $healthUrl -Method Get -TimeoutSec 10 -UseBasicParsing -Headers $headers -ErrorAction Stop
            $contentType = if ($resp.Headers["Content-Type"]) { $resp.Headers["Content-Type"] } else { "" }
            $body = $resp.Content
            if ($contentType -match "json" -and $body -match '^\s*\{' -and $body -notmatch "<!DOCTYPE") {
                $health = $body | ConvertFrom-Json
                $bloomberg = if ($health.bloomberg) { $health.bloomberg.available } else { $false }
                Write-Host "[Initial] Tunnel OK | Bridge: $($health.status) | Bloomberg: $(if ($bloomberg) { 'connected' } else { 'not connected' })" -ForegroundColor Green
            }
        } catch {
            Write-Host "[Initial] Tunnel check failed - $_" -ForegroundColor Yellow
        }
    }

    while (-not $ngrokProcess.HasExited) {
        Start-Sleep -Seconds $checkInterval
        if ($ngrokProcess.HasExited) { break }
        try {
            $headers = @{}
            if ($ngrokUrl -match "ngrok") { $headers["ngrok-skip-browser-warning"] = "1" }
            $resp = Invoke-WebRequest -Uri $healthUrl -Method Get -TimeoutSec 10 -UseBasicParsing -Headers $headers -ErrorAction Stop
            $contentType = if ($resp.Headers["Content-Type"]) { $resp.Headers["Content-Type"] } else { "" }
            $body = $resp.Content
            # Require JSON response - if HTML (ngrok warning page), treat as failure
            if ($contentType -match "json" -and $body -match '^\s*\{' -and $body -notmatch "<!DOCTYPE") {
                $health = $body | ConvertFrom-Json
                $status = $health.status
                $bloomberg = if ($health.bloomberg) { $health.bloomberg.available } else { $null }
                $consecutiveFailures = 0
                $ts = Get-Date -Format "HH:mm:ss"
                if ($bloomberg) {
                    Write-Host "[$ts] Tunnel OK | Bridge: $status | Bloomberg: connected" -ForegroundColor Green
                } else {
                    Write-Host "[$ts] Tunnel OK | Bridge: $status | Bloomberg: not connected" -ForegroundColor Yellow
                }
            } else {
                throw "Response was HTML (ngrok warning?) - not valid JSON"
            }
        } catch {
            $consecutiveFailures++
            $ts = Get-Date -Format "HH:mm:ss"
            Write-Host "[$ts] Tunnel check FAILED ($consecutiveFailures) - $_" -ForegroundColor Red
            if ($consecutiveFailures -ge 3) {
                Write-Host "[$ts] Tunnel may be DOWN. Visit $ngrokUrl in a browser to verify." -ForegroundColor Red
            }
        }
    }
    if ($ngrokProcess.HasExited) {
        Write-Host "Ngrok process ended." -ForegroundColor Yellow
    }
} else {
    Write-Host "Could not detect ngrok URL. Check ngrok output." -ForegroundColor Yellow
    Wait-Process -Id $ngrokProcess.Id -ErrorAction SilentlyContinue
}
