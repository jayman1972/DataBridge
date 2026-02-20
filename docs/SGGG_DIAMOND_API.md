# SGGG Diamond API Setup

The Diamond API provides programmatic access to SGGG fund data (GetPortfolio, GetPortfolioTrades, etc.). It runs **in parallel** with the PSC/ODBC approach—use both as needed.

## Prerequisites

- Credentials from SGGG-FSI (Username, Password)
- Fund ID(s) – GUID for each fund (from EHF GUID.xlsx or SGGG-FSI)
- **IP whitelisting** – Your IP must be whitelisted. Contact apisupport@sgggfsi.com if you get `403 Client IP Address rejected`.

## Configuration

Add to `bloomberg-service.env` (same file as Supabase config):

```
SGGG_DIAMOND_USERNAME=API@EHPARTNERS.COM
SGGG_DIAMOND_PASSWORD=your_password_here
SGGG_DIAMOND_FUND_ID=your-fund-guid-here
```

Get Fund ID(s) from the EHF GUID.xlsx file or from SGGG-FSI during onboarding.

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sggg/diamond/portfolio` | GET/POST | Get finalized portfolio for a fund |
| `/sggg/diamond/trades` | GET/POST | Get portfolio trades for a fund |

### Get Portfolio

- **fund_id** (optional): Override `SGGG_DIAMOND_FUND_ID`
- **date** or **valuation_date**: `yyyy-mm-dd` (default: today)

Example:
```bash
curl -X POST "http://localhost:5000/sggg/diamond/portfolio" \
  -H "Content-Type: application/json" \
  -d '{"valuation_date": "2025-02-12"}'
```

### Get Portfolio Trades

- **fund_id**: Required or `SGGG_DIAMOND_FUND_ID`
- **start_date**, **end_date**: `yyyy-mm-dd` (date range max 1 month)

Example:
```bash
curl -X POST "http://localhost:5000/sggg/diamond/trades" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2025-02-01", "end_date": "2025-02-12"}'
```

## Auth Flow

1. Login with username/password → receive AuthKey
2. AuthKey cached for 1 hour (refresh 5 min before expiry)
3. All subsequent requests send `Authorization: AuthKey` header

## API Base URL

`https://api.sgggfsi.com/api/v1/`

## Support

- API Support: apisupport@sgggfsi.com
- Tech: tech@sgggfsi.com
