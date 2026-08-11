# Data Bridge (Unified)

Unified Data Bridge service for **market-dashboard** and **wealth-scope-ui**.
Replaces the previously scattered implementations in:
- `market-dashboard/data-bridge/`
- `wealth-scope-ui/DataBridge/`
- `wealth-scope-ui/bloomberg-bridge/`

## Quick Start

```batch
cd c:\Users\jmann\projects\DataBridge
start-data-bridge-ngrok.bat
```

Or run from any project:
```batch
# From market-dashboard
c:\Users\jmann\projects\market-dashboard\start-data-bridge-ngrok.bat

# From wealth-scope-ui  
c:\Users\jmann\projects\wealth-scope-ui\start-data-bridge-ngrok.bat
```

## Configuration

Create `bloomberg-service.env` in this folder, or use existing config from:
- `market-dashboard/bloomberg-service.env`
- `wealth-scope-ui/bloomberg-service.env`

Required:
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

Optional:
```
CLARIFI_DIR=C:\Users\YourName\OneDrive\Desktop\EHP_Files\DailyExports from Clarifi\
PORT=5000
# Archived; only set this to reactivate IBKR intentionally:
# DATA_BRIDGE_ENABLE_IBKR=1
```

## Endpoints

| Endpoint | Used By |
|----------|---------|
| `GET /health` | Both projects |
| `POST /bloomberg/quotes` | market-dashboard (portfolio options) |
| `POST /quotes` | wealth-scope-ui |
| `POST /historical` | market-dashboard update |
| `POST /reference` | market-dashboard update |
| `POST /bloomberg-update` | market-dashboard scheduled-update |
| `POST /bloomberg/mergers/refresh` | market-dashboard merger lifecycle update |
| `POST /economic-calendar` | market-dashboard refresh-economic-calendar |
| `GET/POST /sggg/portfolio` | market-dashboard refresh-portfolio |
| `POST /clarifi/process` | market-dashboard update |
| `POST /ehp/process` | market-dashboard update |

The merger lifecycle endpoint refreshes Bloomberg Action fields for open and
recently terminal public-target deals. For stock-funded transactions it also
loads the last raw acquirer price on or before announcement when fixed exchange
terms require it, loads announcement-date `CUR_MKT_CAP` in USD, converts the
reported USD-millions value to absolute dollars, and derives the stock-funding
share and stock-issuance-to-market-cap ratio. Elective or incomplete terms stay
unknown.

## Documentation

- [SGGG API Fields](docs/SGGG_API_FIELDS.md) – Valid fields for the PSC/SGGG position API (for `/sggg/portfolio` and related queries)
- [Archived IBKR Client Portal Gateway](docs/IBKR_GATEWAY.md) – retained setup and reactivation instructions

## IBKR Client Portal Gateway (archived)

IBKR is disabled by default and the unified launcher no longer starts or polls its gateway. The proxy implementation, ignored local gateway directory, and configuration examples remain available for future use. To reactivate it deliberately, set `DATA_BRIDGE_ENABLE_IBKR=1` before launching the bridge. See **[docs/IBKR_GATEWAY.md](docs/IBKR_GATEWAY.md)** for the retained setup.

## Requirements

- Python 3.9+
- Bloomberg Terminal (running & logged in) for Bloomberg endpoints
- OpenVPN + ODBC DSN=PSC_VIEWER for SGGG portfolio
- ngrok (in folder or PATH) for tunneling
- Java (only if the archived IBKR integration is reactivated)

## After Testing

Once verified working, you can delete:
- `market-dashboard/data-bridge/`
- `wealth-scope-ui/DataBridge/`
- `wealth-scope-ui/bloomberg-bridge/`

Keep the launcher batch files in market-dashboard and wealth-scope-ui root - they point here.
