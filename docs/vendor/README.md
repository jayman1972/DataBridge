# SGGG vendor reference (Diamond API)

Official SGGG-FSI materials and response samples used by DataBridge / market-dashboard. Copied from `OneDrive\Desktop\EHP_Files\Temp` on 2026-05-16.

## Files in this folder

| File | Purpose |
|------|---------|
| [Diamond API Specifications v1.17.docx](./Diamond%20API%20Specifications%20v1.17.docx) | Full Diamond API spec (requests, parameters, field definitions). **Authoritative** when code and samples disagree. |
| [GetPortfolio_v1.14.xml](./GetPortfolio_v1.14.xml) | Sample **GetPortfolio** JSON/XML response (`PortfolioRecordDetails` per holding). |
| [GetPortfolioTrades_v1.14.xml](./GetPortfolioTrades_v1.14.xml) | Sample **GetPortfolioTrades** response (`PortfolioTrade` per trade). |

## Repo docs that use these files

| Doc | Location |
|-----|----------|
| Diamond setup (env, bridge routes) | [../SGGG_DIAMOND_API.md](../SGGG_DIAMOND_API.md) |
| Endpoint inventory (no secrets) | [../SGGG_DIAMOND_ENDPOINTS.md](../SGGG_DIAMOND_ENDPOINTS.md) |
| Response field lists (from XML samples) | [../SGGG_DIAMOND_RESPONSE_FIELDS.md](../SGGG_DIAMOND_RESPONSE_FIELDS.md) |
| PSC/ODBC position columns (`psc_position_history`) | [../SGGG_API_FIELDS.md](../SGGG_API_FIELDS.md) |
| Fund Admin / tunnel notes | `market-dashboard/docs/SGGG_API.md`, `market-dashboard/SGGG_PORTFOLIO_TUNNEL.md` |

## Related code

- **DataBridge:** `src/sggg/diamond_client.py`, `data_bridge.py` (`/sggg/diamond/*`)
- **Portfolio refresh (PSC/ODBC):** `sggg_portfolio_route.py`, Edge Function `refresh-portfolio`

## External copies (not in git)

- **Dan White `diamond_api` backup:** `OneDrive\Desktop\EHP_Files\Backup\Dan White GitHub\diamond_api\` — production scripts for accounts, positions, transactions, and additional v1/v2 endpoints (see endpoints doc). **Do not commit** `constants.py` or notebooks that contain passwords.
- **Excel:** `OneDrive\Desktop\EHP_Files\SGGG Viewer - All Funds ver2.xlsm` — fund/portfolio viewer workbook.

## Support

- API: apisupport@sgggfsi.com  
- Tech: tech@sgggfsi.com  
- Base URL: `https://api.sgggfsi.com/api/v1/` (some methods under `/api/v2/`)
