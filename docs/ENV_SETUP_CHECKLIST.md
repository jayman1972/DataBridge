# Environment Setup Checklist (for home PC or new machine)

`bloomberg-service.env` is gitignored (contains secrets). Use this checklist to set it up on another PC.

## File location

Create or edit: `market-dashboard\bloomberg-service.env` (or `DataBridge\bloomberg-service.env` if you run Data Bridge from there).

## Required variables

| Variable | Where to get it |
|----------|-----------------|
| `SUPABASE_URL` | Supabase Dashboard → Project Settings → API |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase Dashboard → Project Settings → API → service_role key |
| `SGGG_DIAMOND_USERNAME` | From SGGG-FSI (e.g. API@EHPARTNERS.COM) |
| `SGGG_DIAMOND_PASSWORD` | From SGGG-FSI |
| `SGGG_DIAMOND_FUND_ID` | From EHF GUID.xlsx or SGGG-FSI |

## Optional

| Variable | Purpose |
|----------|---------|
| `USD_CAD_RATE` | Override for CAD conversion (e.g. 1.35) |
| `GOLDMAN_CLIENT_ID`, etc. | Only if using Goldman Marquee |

## Template (copy and fill in)

```
SUPABASE_URL=https://aphjduxfgsrqswonmgyb.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<from Supabase Dashboard>
SGGG_DIAMOND_USERNAME=API@EHPARTNERS.COM
SGGG_DIAMOND_PASSWORD=<from SGGG-FSI>
SGGG_DIAMOND_FUND_ID=<from EHF GUID.xlsx>
```

## Ways to transfer to home PC

1. **Manual copy** – Open the file on work PC, copy contents, paste into new file on home PC.
2. **OneDrive/cloud** – If you use OneDrive on both PCs, put a copy in a private folder (avoid shared folders).
3. **Password manager** – Store the values in 1Password/Bitwarden/LastPass as a secure note; reference from home.
4. **Secure note to self** – Email yourself an encrypted note or use a temporary secure link (e.g. privnote.com) and delete after use.
5. **USB stick** – Copy the file to a USB drive and transfer (keep the drive secure).

**Do not** commit `bloomberg-service.env` to git or store it in a public repo.
