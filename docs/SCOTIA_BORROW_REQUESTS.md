# Scotia borrow requests

Model Builder → Rebalance → **Borrow requests** downloads the existing manual
TD paste file for non-Scotia short sales and sends Scotia short sales by SFTP.
Current Scotia account routes cover SOAR and Strategic Income. Buying, selling a
long position, and covering a short do not request borrow.

The admin-only `POST /sggg/scotia-borrow-requests` accepts `draft_ids`,
`trade_date`, and `payload_sha256`. It reloads the selected ready drafts and
active prime/account routes from Supabase, validates whole-share quantities and
identifiers, and verifies the reviewed CSV digest before sending. It does not
record a trade export or change a draft's status.

CSV columns match the previous VBA: `CUSIP,ISIN,SEDOL,RIC,COUNTRY,QUANTITY`.
RIC comes from the draft's custodian ID, and may be blank. CAD maps to CA and USD
to US. Rows are preserved without netting across strategies or funds. Filenames
are `EDGEHILL_Scotia_Locates_NA_YYYYMMDD_N.csv`, using the next unused sequence.

## Local configuration

The bridge runs in the existing signed-in Windows session with access to W:.
No credentials are sent to the browser or committed to Git.

| Setting | Default |
| --- | --- |
| `SCOTIA_LOCATE_SCRIPT` | `W:\James Park\EHP Funds\Short Requests\WinSCP_Put_Short_Request_File_Scotia.txt` |
| `SCOTIA_LOCATE_OUTPUT_DIR` | `W:\James Park\EHP Funds\Short Requests\Scotia` |
| `SCOTIA_LOCATE_AUDIT_DIR` | `C:\SGGGPSC\ScotiaBorrowRequests` |

The connection URL is read from the existing WinSCP script. The script itself
is never executed: its `put -latest` wildcard would risk sending another file.
Only `files.prd.scotiabankprime.com:22` is allowed, to `/drop/SL/`. The server
must match the existing trusted WinSCP RSA key in the current user's registry.
A changed or missing server key stops delivery; never automatically trust it.
Paramiko 4.0.0 is required (already installed on the production bridge).

## Delivery audit and recovery

Local CSVs are retained in the existing Scotia folder. `deliveries.sqlite3` in
the separate audit folder stores the submitting administrator, filename, times,
payload SHA-256, and per-draft-row fingerprints. Keep and back up this audit file;
deleting it removes duplicate protection. Do not put it in the Scotia CSV folder
because the legacy Excel wildcard upload might select it.

Repeated or overlapping selections skip unchanged rows already delivered,
including requests sent by another administrator. Changed previously submitted
draft rows require review. SFTP writes an exclusive `.filepart`, checks its size,
then renames it without overwriting an existing final file. A confirmed SFTP
delivery means the file reached Scotia; it does not mean borrow was approved.

A connection/login failure before upload can be retried. An interrupted upload
or missing acknowledgement leaves the request reserved with `sending` or
`review` status. The UI shows its filename and blocks automatic resubmission.
An administrator must inspect Scotia's receipt and both remote filenames before
reconciling the local audit. If Scotia confirms receipt, mark the delivery sent;
if Scotia confirms no request was received and no final file exists, remove only
that delivery's item reservations so the next click can prepare a new sequence.
Retain the original delivery audit record and local CSV for investigation.

## Verification

Run Ruff using the repository configuration and `tests/test_scotia_borrow.py`.
Also run the existing reviewed-trade-save, trade-file-export, and fund-capital
tests. The dashboard tests cover TD separation and a digest of the exact legacy
CSV. Production connectivity is checked by authenticating and reading metadata
for `/drop/SL/`; never send fabricated locate requests as a deployment test.
