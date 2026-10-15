# DataBridge tray launcher

This Windows desktop app keeps the existing `data_bridge_runtime.py` and
`start-tunnel-ngrok.ps1` running in the signed-in user's session without two
console windows. It starts hidden in the system tray and opens its status
window after a sustained connection failure. The status window separates PSC,
Bloomberg, the local ngrok agent, public tunnel reachability, and tunnel URL
synchronization. A transient disconnect is retried; a missing VPN, Bloomberg
login, or failed cloud URL update remains visible for the user to resolve.

The tray app is not a Windows service. It starts **after this user signs in**,
not before sign-in, and it exits on sign-out. This is intentional for the
Bloomberg Desktop API and the account's VPN, ODBC DSN, and file access.

The status window charts the last four hours of tunnel requests in five-minute
bars, using ngrok's local cumulative counter. Health checks are included. The
tray stores only time buckets and counts in
`%LOCALAPPDATA%\EHP\DataBridgeTray\tunnel-requests.json`; it does not store
request URLs or bodies. Earlier time slots are shown as untracked until four
hours of history have been collected. The counts survive a tray restart.

## Prerequisites

- Run on the PSC-connected Windows PC under the account with `PSC_VIEWER`,
  OpenVPN, Bloomberg Terminal, Python dependencies, ngrok, and authenticated
  Supabase CLI access to the configured projects.
- .NET Framework 4.8, including its C# compiler, must be installed.
- Keep the repository at its installation path: the scheduled task starts the
  EXE from `tray/bin` and the EXE locates the repository relative to itself.
- Verify tunnel access control before enabling unattended auto-start. The
  current plain `ngrok http 5000` exposes legacy DataBridge routes without
  application-level authorization. Do not treat a hidden window as a security
  control. The installer refuses auto-start until its
  `-TunnelAccessPolicyVerified` flag is provided after a real access-control
  check; the flag itself does not add protection.

## Build and install

From this folder, run `powershell -ExecutionPolicy Bypass -File .\build.ps1`.
The executable is built into ignored `bin/`; run it with `--self-test` for the
local process-supervision and tunnel-status checks. To register auto-start for
the **current Windows user**, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1 -TunnelAccessPolicyVerified
```

The task runs at that user's sign-in and is configured to restart if the app
crashes. `install.ps1 -StartNow` also starts it immediately, but refuses to do
so while port 5000 is occupied. First close the two manually launched
DataBridge/ngrok windows; do not leave the batch launcher running alongside
the tray app. The app itself will also refuse to take over an occupied bridge
or ngrok management port rather than killing another user's process.

Use `uninstall.ps1` to remove auto-start. Exit the running app from its tray
menu to stop the bridge and tunnel process trees it started. Closing its
status window only hides the window; the bridge keeps running.

Logs are in `%LOCALAPPDATA%\EHP\DataBridgeTray\bridge-tray.log`. A status file
in the same directory records the last tunnel URL and whether the launcher's
existing three-project URL update succeeded. It contains no credentials.

## Operational notes

- The PSC probe is `/sggg/health`, not `/health`: the latter reports Bloomberg
  status and can be unavailable when PSC is healthy.
- The tray app launches the current `start-tunnel-ngrok.ps1` so URL changes are
  propagated exactly as the manual launcher does. If the Supabase CLI is not
  authenticated, or one configured project update fails, the status window
  opens and logs the failure.
- A `DATA_BRIDGE_PYTHON_EXE` environment variable can point at a particular
  Python executable. Otherwise the app runs `python.exe` from `PATH`.
- The app does not create a new VPN connection or sign into Bloomberg; it
  prompts the signed-in user when those prerequisites are disconnected.
