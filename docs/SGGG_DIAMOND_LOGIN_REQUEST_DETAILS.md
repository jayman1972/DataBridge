# SGGG Diamond API – Login Request Details (for SGGG-FSI TECH / apisupport)

We are calling your Diamond API and **POST to `https://api.sgggfsi.com/api/v1/login/` returns HTTP 500** with body:

```json
{"Message":"An error has been encountered, please notify your SGGG-FSI TECH group at apisupport@sgggfsi.com."}
```

Below is exactly what we send so you can verify format and check logs for our account.

---

## 1. Login request (what we send)

**URL:** `POST https://api.sgggfsi.com/api/v1/login/`

**Headers:**
```
Content-Type: application/json
```

**Request body (JSON):**
```json
{
  "username": "API@EHPARTNERS.COM",
  "password": "<our API password – redacted in this doc>"
}
```

- **Username we use:** `API@EHPARTNERS.COM` (from our config).
- **Password:** We use the API password provided to us; it is stored in our env and not included here. You can look up our account by username.

**Note:** We have also tried the same request with PascalCase keys `"Username"` and `"Password"` – same 500 response.

---

## 2. Where these values come from

- Username and password are read from our environment variables `SGGG_DIAMOND_USERNAME` and `SGGG_DIAMOND_PASSWORD` (loaded from a `bloomberg-service.env` file).
- They are passed as plain strings into the JSON body above; no encoding or hashing is applied to the password in the login request.

---

## 3. Code that performs the login (Python)

```python
# From: DataBridge/src/sggg/diamond_client.py

BASE_URL = "https://api.sgggfsi.com/api/v1"

def _login(self) -> str:
    url = f"{self.base_url}/login/"
    payload = {"username": self.username, "password": self.password}
    resp = requests.post(url, json=payload, timeout=30)
    # resp.status_code == 500, resp.json() == {"Message": "An error has been encountered, ..."}
```

- Library: Python `requests`.
- `requests.post(..., json=payload)` sends the body as UTF-8 JSON with `Content-Type: application/json`.

---

## 4. What we do after login (for context)

Once we have an `AuthKey` from a successful login response, we call:

- `POST https://api.sgggfsi.com/api/v1/GetPortfolio/`
- Headers: `Authorization: Bearer <AuthKey>`, `Content-Type: application/json`
- Body: `{"FundID": "<guid>", "ValuationDate": "2025-02-12", ...}`

We never get that far because login returns 500.

---

## 5. What we need from you

1. Confirm whether the **login** request format above is correct (URL, headers, JSON keys `username`/`password` or `Username`/`Password`, and that the password should be sent in plain text in the JSON body).
2. Check your logs for requests to `/api/v1/login/` with username **API@EHPARTNERS.COM** and confirm the exact server-side error or reason for the 500.
3. Confirm that this API user is enabled for Diamond API access and that our IP (we can provide if needed) is allowed.

Thank you.
