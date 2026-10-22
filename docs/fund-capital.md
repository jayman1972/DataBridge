# Verified production fund capital

The service-authorized `GET /sggg/fund-capital?fund=EHP%20Alpha&date=20261021`
reads `psc_hist_group_portfolio_nav` by exact portfolio ID and date. It returns
pre-transfer `NAV`, same-date `Transfers_In`, and their sum as `fund_nav`, with
a versioned `capital_basis` audit. Missing or ambiguous records fail closed.

Only `/sggg/portfolio?profile=production_snapshot` is enriched with this capital
basis. General portfolio and Diamond reconciliation retain their original NAV.
Position-level transfer columns have different timing and are not substituted.

AlphaDesk file saves additionally require a persisted audit for the signed-in
administrator, filename and payload hash. The bridge verifies the prior XTSE
session, current fund NAV/flows and trade-date additional cash adjustment before
writing a file. Old drafts without proof must be rerun. This guard does not
generate trades, rewrite drafts, or alter historical fund records.
