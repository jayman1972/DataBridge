# Corporate-action lookup route

`data_bridge_runtime.py` registers `POST /bloomberg/corporate-actions-lookup`.
It requires the configured Supabase service credential as a bearer token. The
market-dashboard Edge Function verifies user/portfolio roles before calling it.
It is not a new unauthenticated tunnel route.

The route is on-demand only: 1–10 input identifiers, at most 500 expanded
securities, at most 1,000 Action IDs per family, ordered dates spanning at most
800 days, and a 105-second total request deadline. Identical successful request
snapshots are retained in a local SQLite cache and reused for five minutes.
Failed requests are not cached. The lock permits one lookup at a time without
interfering with legacy incident or daily corporate-action workflows.

It preserves structured Bloomberg fields, provider errors, source mapping and
coverage warnings. `RELATED_EQUITIES` plus issuer/primary-equity mappings is an
API-derived approximation, not exact CACT Security Plus and not ETF holdings.
Listings/delistings and name/ticker changes are unsupported. Missing effective
dates are never replaced with announcement or buyback-expiration dates.
Pending/proposed M&A expected dates are explicitly labeled.

Run `ruff check` on the new modules, runtime entry point and tests, then:

```powershell
$env:PYTHONPATH='src'
python -m unittest discover -s tests -p '*corporate*actions*.py'
```

The implementation reuses the production tray-supervised runtime. After
publishing, restart only that exact verified runtime child; the existing tray
supervisor restarts it. Verify `/health` reports
`2026-10-20-corporate-actions-lookup`, unauthenticated access is denied, and a
small authenticated Bloomberg lookup succeeds.
