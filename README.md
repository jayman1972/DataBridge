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

## Requirements

- Python 3.9+
- Bloomberg Terminal (running & logged in) for Bloomberg endpoints
- OpenVPN + ODBC DSN=PSC_VIEWER for SGGG portfolio
- ngrok (in folder or PATH) for tunneling

## After Testing

Once verified working, you can delete:
- `market-dashboard/data-bridge/`
- `wealth-scope-ui/DataBridge/`
- `wealth-scope-ui/bloomberg-bridge/`

Keep the launcher batch files in market-dashboard and wealth-scope-ui root - they point here.
