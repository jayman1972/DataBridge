# HAL maintenance caller (Jason-W11)

After the existing attachment automation saves and verifies Greg's `OilDemand.csv`, call:

```powershell
& 'C:\Users\jmann\AppData\Local\Programs\Python\Python312\python.exe' 'C:\Tasks\hal_maintenance_jobs.py' run --oil-csv 'C:\Users\jmann\OneDrive\Desktop\EHP_Files\DailyExports from Clarifi\OilDemand.csv'
```

The command waits for Clarifi and its EHP files, then starts Calculated fields using the server's 20-day recent-window path. It returns JSON with both `runIds`, run summaries, and `signal`. Send a Rising/Falling notification only when the top-level `notify` is `true`; use `signal.transitionId` as the notification's idempotency key. The caller records that key before returning it, so repeating the same cycle will not ask HAL to notify again. It does not save attachments, watch email, or change the existing schedule.

Check progress or investigate an error without starting a job:

```powershell
$status = & 'C:\Users\jmann\AppData\Local\Programs\Python\Python312\python.exe' 'C:\Tasks\hal_maintenance_jobs.py' status | ConvertFrom-Json
$status.runs.clarifi
$status.runs.'calculated-fields'
& 'C:\Users\jmann\AppData\Local\Programs\Python\Python312\python.exe' 'C:\Tasks\hal_maintenance_jobs.py' status --run-id $status.runs.clarifi.runId
& 'C:\Users\jmann\AppData\Local\Programs\Python\Python312\python.exe' 'C:\Tasks\hal_maintenance_jobs.py' probe
```

`status` reports completion, errors, warning counts/messages and current step. A timed-out `run` can be invoked again with the same CSV; it resumes known run IDs. If a start is uncertain and cannot be reconciled with the server's latest run, the caller stops and requires manual review instead of retrying. After resolving a *known completed failure*, an operator can explicitly clear that job's failed run record, then invoke `run` again:

```powershell
& 'C:\Users\jmann\AppData\Local\Programs\Python\Python312\python.exe' 'C:\Tasks\hal_maintenance_jobs.py' retry-failed --job-key clarifi
& 'C:\Users\jmann\AppData\Local\Programs\Python\Python312\python.exe' 'C:\Tasks\hal_maintenance_jobs.py' run
```

Use `--job-key calculated-fields` to retry only that stage. The reset refuses uncertain, active, or successful runs. A failed or warning-bearing Clarifi/EHP run prevents Calculated fields from starting. A mismatch between the saved CSV and persisted oil value, a stale oil date, or a non-binary indicator prevents notification. The state file is `C:\Tasks\hal_maintenance_state.json`; do not remove it to retry a start because it holds run IDs and notification deduplication state.

The caller reads DataBridge's existing server credential internally from its configured environment or `bloomberg-service.env`. Never pass the credential on the command line or include it in logs.
