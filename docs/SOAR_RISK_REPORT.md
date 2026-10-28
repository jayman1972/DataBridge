# SOAR weekly risk report

`data_bridge_runtime.py` serves the independent three-page SOAR PDF at
`POST /sggg/risk-reports/soar`. The route requires the bridge service key and is
intended to be called by the authenticated Fund Admin Edge Function. A request
body contains `as_of` (`YYYY-MM-DD`) and optionally `save_to_disk: true`.
Without the save flag, the response is the PDF. With it, the PDF is saved to
`\\ehp04\workgroups\James Park\EHP Funds\Risk\Reports - Internal` (override
with `SOAR_RISK_REPORTS_DIR`) and the response confirms the filename. Existing
reports are never replaced silently.

The bridge process starts a weekly worker at launch. Starting Monday at 10:00
Toronto time, it checks every five minutes for the preceding Friday's report
and continues through Friday if a required source is late. It does nothing on
weekends. It can run only while the signed-in DataBridge tray process and its
Diamond, PSC, Bloomberg, Supabase, and network-share connections are available.
If a report fails, no partial PDF is saved; the first occurrence of each
distinct failure is logged. The next week's worker targets the new Friday.

To generate a review copy without saving to the official directory:

```powershell
python scripts/run_soar_risk_report.py --date 2026-10-23 --output C:\Temp\soar-review.pdf
```

Use `--save` only after reviewing the source date and output. The generator
does not execute the Excel workbook. Its sources and calculation rules are:

- Fund NAV, Class B (950B) NAV per unit, period performance, and ETF positions:
  Diamond SOAR parent `41010000-7f79-9c86-0bd2-45484608d9e1`.
- Weekly filled trades: PSC account `BNS-SOAR LS`. Cash and CAD/USD spot and
  forward entries are excluded from the table.
- Realized daily return and one-day 95% historical VaR: PSC daily P&L divided
  by prior NAV plus transfers, using the empirical fifth percentile of a full
  trailing year with at least 240 daily observations. The old manually entered
  VaR is not carried forward.
- ETF beta, 20-day volume, short interest, benchmark prices, and historic ETF
  scenario prices: Bloomberg. Scenario P&L applies each historic ETF move to
  the current signed portfolio weight; it is an estimate, not a forecast.
- Model-vs-actual chart: the legacy SOAR TXT through 2026-10-16, the
  user-supplied 2026-10-19 through 2026-10-23 bridge CSV, then the production
  `SOAR Fund (Production Backtest)` reference-portfolio schedule. A missing
  current model observation prevents report generation.

The 2026-10-23 report is the audited handoff for the Class B since-inception
annualized series. Subsequent finalized Diamond valuation-period returns are
chained from it. The original PDF's trade-execution confirmation is retained
as a legacy statement in the layout; generating this report does not itself
verify trade approvals. Report operators remain responsible for that
attestation before distribution.

Diamond can return a later valuation period than the requested position date
or delay finalizing a period. The report deliberately fails rather than mix
NAV from one date with positions, trades, and risk data from another. This may
delay a weekly PDF, especially near month-end, and should be monitored in the
bridge log.
