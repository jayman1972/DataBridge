# On-demand corporate-action Excel jobs

The market-dashboard Security Details page submits work through the authenticated
`corporate-actions-lookup` edge function. It supports `start`, `status`, and
`cancel` operations and forwards the verified Supabase user ID to DataBridge in
`X-Requester-Id`, together with the existing server credential. The browser never
receives that credential. A job can only be read or stopped by its requester.

DataBridge persists requests, progress, and results in
`%LOCALAPPDATA%\EHP\DataBridge\corporate-actions-jobs\jobs.sqlite3`. Each job has
an unpredictable ID and a separate folder for its workbook, raw returned tables,
and worker log. These results are local to the Bloomberg PC, not a new Supabase
table. An idempotent request ID prevents duplicate submissions from starting the
same batch twice. The queue accepts at most four unfinished batches and executes
one at a time. Jobs interrupted by a bridge restart are marked failed.

## Coverage

- Existing API-backed action families continue to use the reviewed BLPAPI lookup.
- Listings, delistings, and ticker changes use Excel BQL with `showallcols=t`.
- Queries use a value-only `Inputs` range through `BQL.LIST`; arbitrary formulas,
  workbook paths, scripts, and BQL items are not accepted from app requests.
- Each request has at most ten source identifiers, 500 resolved securities, an
  800-day date range, and 50 securities per Excel query. A table is capped at
  10,000 rows. The Excel child is bounded to 15 minutes.
- `EFFECTIVE_DATE` and `ANNOUNCED_DATE` determine the event date. BQL's `DATES`
  column is an observation date and must never substitute for either one.
- The exact, live-observed `#N/A No Data: Function ... returned empty results.`
  response is accepted as empty only when it names the requested allowlisted
  query. Generic `#N/A`, invalid parameters, missing columns, entitlement errors,
  missing event dates, and timeouts remain visible.
- These tables do not expose an Action ID. Events use a stable hash of their
  security, action family, dates, and terms. An Action ID is never fabricated.
- Name-change history is not included. Issuer and Security Plus expansion remain
  approximate and are not certified CACT-native equivalents. These limitations
  are shown in the app and exported coverage warnings.

## Excel ownership and scheduling

The worker copies the committed, macro-free template, launches a new Excel
instance with `/x /r`, and binds only an already-registered workbook whose full
path and process ID match that launch. It never uses `GetObject(path)`, which can
activate a workbook in a pre-existing user instance during startup.

Only the job-owned workbook is calculated and closed. No application-wide
refresh, calculation-mode, alert, event, security, or macro settings are changed.
Excel is quit only when the matching process has no remaining workbooks. On a
hard timeout only the job's Python child is terminated; an unclosed job-owned
Excel instance is reported rather than risking termination of user work.

App Excel jobs acquire `EHP_BQL_CORPORATE_ACTIONS` and honor the existing
`EHP_BQL_BEST_DPS_BACKFILL` mutex. Daily/merger scheduler scripts are not changed
and do not acquire the new mutex; their workbook/process isolation remains the
boundary. No existing scheduled publish task is triggered by this endpoint.

The Windows session must remain signed in, with Bloomberg logged in, the PC
awake, and DataBridge running. Excel is minimized, not a headless server. The
template requires the installed Bloomberg Excel add-in and pywin32 (311 or newer).

## Verification

Live isolated-worker checks reproduced ARM's 2023-09-14 listing, Twitter's
2022-10-28 delisting, and Meta's 2022-06-09 ticker change. Announcement-basis
filtering returned Meta's Bloomberg announcement date of 2021-10-28. Separate
tests cover idempotency, requester isolation, cancellation, queue limits,
interrupted jobs, provider errors, date parsing, and Excel cleanup ownership.
