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
| `POST /economic-calendar` | market-dashboard refresh-economic-calendar |
| `GET/POST /sggg/portfolio` | market-dashboard refresh-portfolio |
| `POST /clarifi/process` | market-dashboard update |
| `POST /ehp/process` | market-dashboard update |

## Documentation

- [SGGG API Fields](docs/SGGG_API_FIELDS.md) – Valid fields for the PSC/SGGG position API (for `/sggg/portfolio` and related queries)

## IBKR Client Portal Gateway (optional)

To use the IBKR retail API (portfolio, market data, orders) through the Data Bridge:

1. Download the [Client Portal API Gateway](https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/) (Java) and unzip it.
2. Place it in this repo as `IBRK`, or set `IBKR_GATEWAY_DIR` to its path.
3. Edit `root/conf.yaml` in the Gateway folder: set **`listenPort: 5001`** (Data Bridge uses 5000).
4. Run `start-data-bridge-ngrok.bat`; it will start the Gateway in a new window, then the Data Bridge and ngrok.
5. Log in once per day at https://localhost:5001 in your browser. The Data Bridge can then call `https://localhost:5001/v1/api/...` to talk to IBKR and push data to Supabase.

## Requirements

- Python 3.9+
- Bloomberg Terminal (running & logged in) for Bloomberg endpoints
- OpenVPN + ODBC DSN=PSC_VIEWER for SGGG portfolio
- ngrok (in folder or PATH) for tunneling
- Java (for IBKR Gateway, if using)

## After Testing

Once verified working, you can delete:
- `market-dashboard/data-bridge/`
- `wealth-scope-ui/DataBridge/`
- `wealth-scope-ui/bloomberg-bridge/`

Keep the launcher batch files in market-dashboard and wealth-scope-ui root - they point here.
