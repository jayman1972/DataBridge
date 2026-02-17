# SGGG / PSC Position API Fields

Reference for valid fields from the SGGG (PSC) position history API. Use when querying `psc_position_history` or similar views via ODBC (DSN=PSC_VIEWER).

## Field Schema

| Field | Type | Null | Key | Default | Comment |
|-------|------|------|-----|---------|---------|
| SECURITY | varchar(20) | NO | | | |
| DESCRIPTION | varchar(50) | YES | | | |
| SEC_CCY | varchar(3) | NO | | | The settlement Currency - i.e. currency of price quote |
| SECURITY_SN | varchar(50) | NO | | | Short name |
| CUSIP | varchar(20) | YES | | | |
| ISIN | varchar(20) | YES | | | |
| SEDOL | varchar(20) | YES | | | |
| OTHER_ID | varchar(50) | YES | | | |
| BBG_TICKER | varchar(50) | YES | | | |
| RIC | varchar(50) | YES | | | |
| SECURITY_TYPE | varchar(20) | NO | | | |
| EXCHANGE | varchar(20) | YES | | | |
| QUOTE_SIZE | decimal(19,4) | NO | | | |
| CONTRACT_SIZE | decimal(19,4) | NO | | | |
| UNDERLYING_SN | varchar(50) | NO | | | ID for underlying company stock |
| COMPANY_SYMBOL | varchar(20) | YES | | | Root symbol for ticker - symbol and exchange should be sufficient to generate any ticker |
| SECTOR | varchar(20) | YES | | | |
| INDUSTRY | varchar(20) | YES | | | |
| USER_DEF1 | varchar(20) | YES | | | |
| USER_DEF2 | varchar(20) | YES | | | |
| USER_DEF3 | varchar(20) | YES | | | |
| USER_DEF4 | varchar(20) | YES | | | |
| USER_DEF5 | varchar(20) | YES | | | |
| USER_DEF6 | varchar(20) | YES | | | |
| COMPLIANCE_TAG1 | varchar(20) | YES | | | |
| COMPLIANCE_TAG2 | varchar(20) | YES | | | |
| COMPLIANCE_TAG3 | varchar(20) | YES | | | |
| COUNTRY | varchar(20) | YES | | | Country of Exposure - e.g which country's economy etc will affect value |
| PRICE_SOURCE | int(11) | YES | | | |
| POSN_DATE | int(11) | NO | | | |
| LONG_SHORT | varchar(5) | NO | | | |
| QUANTITY | decimal(20,4) | YES | | | |
| AVG_PRICE | decimal(19,9) | NO | | | |
| CLOSE_PRICE | decimal(19,9) | NO | | | |
| PRICE_PROFIT | decimal(20,4) | NO | 0 | | |
| INTEREST | decimal(19,4) | YES | | | |
| DIVIDENDS | decimal(19,4) | YES | | | |
| FEES | decimal(19,4) | YES | | | |
| FX_SETTLE_TO_BASE | decimal(19,9) | YES | | | |
| VALUE | decimal(19,4) | YES | | | |
| EXPOSURE | decimal(19,4) | YES | | | |
| DAY_PROFIT | decimal(19,4) | YES | | | |
| SUBSCRIPTIONS | decimal(19,4) | YES | | | |
| STRATEGY | varchar(20) | NO | | | |
| OWNER | varchar(20) | NO | | | |
| TRADE_GROUP | varchar(20) | NO | | | |
| USER_GROUP1 | varchar(20) | NO | | | |
| USER_GROUP2 | varchar(20) | NO | | | |
| USER_GROUP3 | varchar(20) | NO | | | |
| POSN_STATUS | varchar(7) | NO | | | |
| FX_EXPOSURE_LOC | decimal(20,4) | NO | 0 | | |
| DAY_FX_PROFIT | decimal(19,4) | NO | | | |
| FX_SECURITY_TO_BASE | decimal(19,9) | YES | | | |
| FX_BASE_TO_DENOM | decimal(19,9) | YES | | | |
| ACCR_INT | decimal(19,4) | YES | 0 | | |
| FUT_TOT_PRICE | decimal(19,4) | YES | 0 | | |
| POSN_OPEN_DT | int(11) | NO | 0 | | |
| POSN_CLOSE_DT | int(11) | NO | 0 | | |
| ACCOUNT_ID | int(11) | NO | | | |
| ACCOUNT | varchar(20) | NO | | | |
| ACCOUNT_DESCRIPTION | varchar(50) | YES | | | |
| PORTFOLIO | varchar(20) | NO | | | |
| PORTFOLIO_DESCRIPTION | varchar(50) | YES | | | |
| PORTFOLIO_CCY | varchar(3) | NO | | | |
| PORTFOLIO_NAV | decimal(19,4) | NO | | | |
| PORTFOLIO_TRANSFERS | decimal(19,4) | YES | | | |
| PREV_DAY_NAV | decimal(19,4) | NO | | | |
| PREV_MONTH_END_NAV | decimal(19,4) | NO | | | |
| POSN_DATE_INT | int(11) | NO | | | |
| PREV_POSN_DATE_INT | int(11) | NO | | | |
| PREV_MONTH_END_INT | int(11) | NO | | | |
| POSN_MONTH | varchar(20) | NO | | | |
| ISSUER_GROUP | varchar(50) | YES | | | |
| COMPLIANCE_OVERRIDE | varchar(50) | YES | | | |
| ISSUER_CLASSIFICATION | varchar(50) | YES | | | |
| SECURITY_CLASSIFICATION | varchar(50) | YES | | | |
| ACCOUNT_NUMBER | varchar(50) | YES | | | |
| LAST_PRICE_SOURCE | varchar(20) | YES | | | |
| MANUAL_PRICE | decimal(19,9) | YES | | | |
| SPREAD_SOURCE | varchar(20) | YES | | | |
| MANUAL_SPREAD | decimal(19,9) | YES | | | |
| UNDERLYING_SECURITY | varchar(20) | YES | | | |
| UNDERLYING_DESCRIPTION | varchar(50) | YES | | | |
| UNDERLYING_SEC_CCY | varchar(3) | YES | | | The settlement Currency - i.e. currency of price quote |
| UNDERLYING_CUSIP | varchar(20) | YES | | | |
| UNDERLYING_ISIN | varchar(20) | YES | | | |
| UNDERLYING_SEDOL | varchar(20) | YES | | | |
| UNDERLYING_OTHER_ID | varchar(50) | YES | | | |
| UNDERLYING_EXCHANGE | varchar(20) | YES | | | |
| UNDERLYING_QUOTE_SIZE | decimal(19,4) | YES | | | |
| UNDERLYING_CONTRACT_SIZE | decimal(19,4) | YES | | | |
| UNDERLYING_COMPANY_SYMBOL | varchar(20) | YES | | | Root symbol for ticker - symbol and exchange should be sufficient to generate any ticker |
| UNDERLYING_SECTOR | varchar(20) | YES | | | |
| UNDERLYING_INDUSTRY | varchar(20) | YES | | | |
| UNDERLYING_USER_DEF1 | varchar(20) | YES | | | |
| UNDERLYING_USER_DEF2 | varchar(20) | YES | | | |
| UNDERLYING_USER_DEF3 | varchar(20) | YES | | | |
| UNDERLYING_USER_DEF4 | varchar(20) | YES | | | |
| UNDERLYING_USER_DEF5 | varchar(20) | YES | | | |
| UNDERLYING_USER_DEF6 | varchar(20) | YES | | | |
| UNDERLYING_COMPLIANCE_TAG1 | varchar(20) | YES | | | |
| UNDERLYING_COMPLIANCE_TAG2 | varchar(20) | YES | | | |
| UNDERLYING_COMPLIANCE_TAG3 | varchar(20) | YES | | | |
| UNDERLYING_COUNTRY | varchar(20) | YES | | | Country of Exposure - e.g which country's economy etc will affect value |
| UNDERLYING_PRICE_SOURCE | int(11) | YES | | | |

## Key Fields for Portfolio Integration

- **BBG_TICKER** – Bloomberg ticker; use for options and securities when requesting live quotes
- **COMPANY_SYMBOL** – Root symbol; fallback for options when BBG_TICKER is null
- **SECURITY_TYPE** – e.g. Stock, EquityOption, LeveragedETF, Futures
- **SEC_CCY** – Settlement currency (e.g. USD, CAD)
- **QUANTITY** – Position size
- **AVG_PRICE**, **CLOSE_PRICE** – Cost and last close
- **VALUE**, **EXPOSURE** – Position value and exposure
- **DAY_PROFIT** – Daily P&L
- **STRATEGY**, **TRADE_GROUP** – Grouping
- **PORTFOLIO_NAV** – Fund NAV

## Notes

- Date fields (POSN_DATE, etc.) may be stored as integers (e.g. YYYYMMDD).
- Add more fields as needed for new integrations.
- See `sggg_portfolio_route.py` for the current SELECT used by the /sggg/portfolio endpoint.
