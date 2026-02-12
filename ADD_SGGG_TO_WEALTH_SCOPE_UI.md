# Add /sggg/portfolio to wealth-scope-ui DataBridge

You run the Data Bridge from **wealth-scope-ui** at work:
`C:\Users\jmann\wealth-scope-ui\DataBridge\start-data-bridge-ngrok.bat`

That app doesn’t have `/sggg/portfolio` yet. Add it as follows.

## 1. Copy the route file

Copy this file from market-dashboard into wealth-scope-ui:

- **From:** `market-dashboard\data-bridge\sggg_portfolio_route.py`
- **To:** `C:\Users\jmann\wealth-scope-ui\DataBridge\sggg_portfolio_route.py`

## 2. Register the blueprint in the Data Bridge app

Open the main Flask app in wealth-scope-ui DataBridge. The batch runs `python data-bridge.py`, so the main file is likely **`data-bridge.py`** or **`data_bridge.py`** in `C:\Users\jmann\wealth-scope-ui\DataBridge\`.

**A. Add the import** (with the other imports at the top):

```python
from sggg_portfolio_route import sggg_bp
```

**B. Register the blueprint** (after the Flask `app` is created, e.g. after `app = Flask(__name__)` and any other setup):

```python
app.register_blueprint(sggg_bp)
```

## 3. Optional: ensure pyodbc is installed

On the work PC:

```bash
pip install pyodbc
```

(You already have this.)

## 4. Restart the Data Bridge

1. Close the “Data Bridge Service” window (or stop the process on port 5000).
2. Run again: `C:\Users\jmann\wealth-scope-ui\DataBridge\start-data-bridge-ngrok.bat`
3. In the startup log you should see the new route (or at least `GET /sggg/portfolio` will respond).

## 5. Refresh Portfolio

In market-dashboard, use **Refresh Portfolio**. The Edge Function will call your tunnel; the bridge will serve `/sggg/portfolio` and return NAV/positions from PSC (with OpenVPN connected and DSN `PSC_VIEWER` configured).
