# SGGG Diamond API — endpoint inventory

Consolidated from **DataBridge** (`diamond_client.py`), **Dan White `diamond_api`** (`write_sggg_data_to_db.py`, `scrape_diamond.py`), and notebooks under `EHP_Files\Backup\Dan White GitHub\diamond_api`. No credentials here — configure via `SGGG_DIAMOND_USERNAME` / `SGGG_DIAMOND_PASSWORD` in `bloomberg-service.env`.

**Spec:** [vendor/Diamond API Specifications v1.17.docx](./vendor/Diamond%20API%20Specifications%20v1.17.docx)

## Authentication

| Method | URL | Body / notes |
|--------|-----|----------------|
| POST | `https://api.sgggfsi.com/api/v1/login/` | `Username`, `Password` (PascalCase in legacy scripts; `diamond_client.py` also accepts lowercase JSON). Response: `AuthKey` (1 hour). |
| Header | Subsequent calls | `Authorization: AuthKey <token>` and/or `AuthKey: <token>` |

## Implemented in DataBridge today

| Method | URL | Used by |
|--------|-----|---------|
| POST | `/api/v1/GetPortfolio/` | `diamond_client.get_portfolio`, `/sggg/diamond/portfolio` |
| POST | `/api/v1/GetPortfolioTrades/` | `diamond_client.get_portfolio_trades`, `/sggg/diamond/trades` |
| POST | `/api/v1/GetNAVSheet/` | `diamond_client.get_nav_sheet` |
| POST | `/api/v1/GetFundDetails/` | `diamond_client.get_fund_details` |

**GetPortfolio body (typical):** `FundID` (GUID), `ValuationDate` (`yyyy-mm-dd`), optional `ReferenceDate`, `ExcludeFlatPositions`, `ExcludeNotPricedPositions`.

**GetPortfolioTrades body:** `FundParentID`, `StartDate`, `EndDate`, `DateType` (`ValuationDate` / `SettlementDate` / `TradeDate` / `ProcessDate`).

## Additional endpoints (legacy `diamond_api` / notebooks)

These were used for dealer-account EHF workflows; paths follow `scrape_diamond.get_method_dict()` conventions.

### Fund-level (`/api/v1/{Method}/`)

| Method | Notes |
|--------|--------|
| GetBalanceSheet | `FundID`, `ValuationDate` |
| GetNAVSheet | Same |
| GetForwardsSchedule | Same |
| GetPortfolio | Same |
| GetPortfolioTrades | Date range on fund parent |
| GetPendingOrders | Per spec |
| GetFundDetails | `FundID` |

### Accounts (`/api/v1/Accounts/{Method}/`)

| Method | Notes |
|--------|--------|
| GetAccountsComprehensiveInformation | Dealer/rep filtering in legacy code |
| GetAccountPositions | `HistoricalDate` (`yyyy-mm-ddT00:00:00`), `DateType` (`T` = trade date in legacy) |
| GetDealersAndReps | Per spec |

### v2

| Method | URL | Notes |
|--------|-----|--------|
| GetProcessedTransactions | `/api/v2/GetProcessedTransactions/` | `StartDate`, `EndDate`; response `GetProcessedTransactionsResponse` |

Legacy v1 alias in scrape list: `GetProcessedTransactionsV2` → `/api/v2/GetProcessedTransactions/`.

## PSC / ODBC (parallel path, not Diamond HTTP)

Portfolio **Refresh** in market-dashboard uses Data Bridge **`GET /sggg/portfolio`** → ODBC `DSN=PSC_VIEWER` / `psc_position_history`. Column list: [SGGG_API_FIELDS.md](./SGGG_API_FIELDS.md).

## Fund IDs

Use parent-fund **GUID** values (not share class codes like BOCA/T0A). Configure `SGGG_DIAMOND_FUND_IDS` (comma-separated). Legacy fund labels in Dan White `constants.py` comments: Multi-Strat, Advantage, Foundation, Select, Strategic Income, etc. — cross-check against **EHF GUID** spreadsheet or **SGGG Viewer** workbook on OneDrive.

## IP whitelisting

403 `Client IP Address rejected` → contact apisupport@sgggfsi.com.
