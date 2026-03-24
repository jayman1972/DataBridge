# IBKR Client Portal Gateway (local install)

The Data Bridge can proxy to Interactive Brokers’ **Client Portal API** via the official Java gateway. The gateway itself is **not** committed to this repo; install it on each machine from IBKR’s download.

## Download

- **ZIP (official):** [clientportal.gw.zip](http://download2.interactivebrokers.com/portal/clientportal.gw.zip)  
- **Overview / docs:** [Client Portal Web API](https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/) and [CP Web API guide](https://interactivebrokers.github.io/cpwebapi)

## Requirements

- Java 8u192+ (OpenJDK 11 is commonly used; see IBKR’s `doc/GettingStarted.md` inside the zip).

## Install layout

1. Unzip the archive.
2. Either:
   - Place the extracted folder at **`DataBridge/IBRK`** (default expected by `start-data-bridge-ngrok.bat`), or  
   - Put it anywhere and set **`IBKR_GATEWAY_DIR`** to that path.

Expected layout after unzip (names may match IBKR’s release):

- `bin/run.bat` / `bin/run.sh`
- `root/conf.yaml` (and related `root` files)
- `dist/`, `build/`, `doc/`

## Configuration (port 5001)

The Data Bridge listens on **5000** by default. The gateway must use a **different** port (this repo uses **5001**).

1. Copy [`ibkr-gateway-conf.example.yaml`](ibkr-gateway-conf.example.yaml) to **`root/conf.yaml`** inside your gateway folder (or merge the important keys—especially `listenPort: 5001`—into the stock `conf.yaml`).
2. Start the gateway, e.g. from the gateway root:

   ```batch
   bin\run.bat root\conf.yaml
   ```

3. Open **https://localhost:5001** in a browser and complete IBKR login when prompted.

## Starting with the unified launcher

From `DataBridge`, run `start-data-bridge-ngrok.bat`. If `IBKR_GATEWAY_DIR` (or default `DataBridge\IBRK`) contains `bin\run.bat`, the script starts the gateway, waits briefly, then opens the login URL.

## Data Bridge proxy routes

These are implemented on the Data Bridge (port **5000**); they forward to the gateway with rate limiting:

- `GET /ibkr/auth-status`
- `GET /ibkr/snapshot?conids=...&fields=...`
- `GET /ibkr/history?conid=...&period=...&bar=...` (optional query params per IBKR API)
- `GET /ibkr/search?symbol=...`

Override the gateway base URL with **`IBKR_GATEWAY_URL`** if it is not `https://localhost:5001`.

## Session cookie (401 from `/ibkr/*`)

The gateway session is tied to the browser login. If server-side calls get **401**, copy the **Cookie** header from DevTools after logging in at `https://localhost:5001` (e.g. `cp=...; SBID=...`) into **`IBKR_SESSION_COOKIE`** in the environment or `bloomberg-service.env`, then restart the Data Bridge.

## Local files to never commit

Runtime noise lives under the gateway folder (e.g. **`IBRK/logs/`**, **`.vertx/`** caches). The repo **`.gitignore`** ignores the whole **`IBRK/`** directory so a local install is never staged by mistake.
