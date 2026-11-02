# Setup Guide — Hourly Merchant Dashboard on GitHub Actions

This runs `hourly_merchant_dashboard_etl.py` automatically every hour on GitHub's servers — no server or laptop of yours needs to stay on. It authenticates to Metabase with your regular email/password (no admin/API-key access needed) and writes results to a Google Sheet.

Files involved (already created in this folder):
- `hourly_merchant_dashboard_etl.py` — the ETL script
- `requirements.txt` — its Python dependencies
- `.github/workflows/hourly_merchant_dashboard.yml` — the schedule definition

---

## Step 1 — Put this folder in a GitHub repository

If you don't already have a repo for this:

1. Go to github.com → click **New repository**. Name it something like `merchant-dashboard-etl`. Keep it **Private** (it will reference your Metabase and sheet indirectly via secrets, though secrets themselves are never exposed).
2. On your computer, open a terminal in this project folder and run:
   ```
   git init
   git add .
   git commit -m "Hourly merchant dashboard ETL"
   git branch -M main
   git remote add origin https://github.com/<your-username>/merchant-dashboard-etl.git
   git push -u origin main
   ```
   (Replace `<your-username>` and the repo name with your actual GitHub path — GitHub shows you this exact command block right after creating the repo.)

If you already have a repo for this project, just copy these three files/folders into it and push.

---

## Step 2 — Create a Google service account (so the script can write to Sheets)

1. Go to [console.cloud.google.com](https://console.cloud.google.com) → create a project (or reuse one).
2. Enable the **Google Sheets API** for that project (search "Google Sheets API" in the top search bar → Enable).
3. Go to **IAM & Admin → Service Accounts → Create Service Account**. Give it any name (e.g. `sheets-writer`). Skip granting it project-level roles — it doesn't need any.
4. Open the new service account → **Keys** tab → **Add Key → Create new key → JSON**. This downloads a `.json` file — keep it safe, it's a credential.
5. Open your target Google Sheet → click **Share** → paste the service account's email (looks like `sheets-writer@your-project.iam.gserviceaccount.com`, found inside the JSON file as `client_email`) → give it **Editor** access.

---

## Step 3 — Base64-encode the service account key

GitHub Secrets store plain text best when it's a single line, so we encode the JSON file first.

- **Mac/Linux:** `base64 -i service-account-key.json | tr -d '\n'`
- **Windows (PowerShell):** `[Convert]::ToBase64String([IO.File]::ReadAllBytes("service-account-key.json"))`

Copy the long output string — you'll paste it into a secret in the next step.

---

## Step 4 — Add GitHub repo secrets

In your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**. Add each of these one at a time:

| Secret name | Value |
|---|---|
| `METABASE_URL` | Your Metabase base URL, e.g. `https://metabase.fairdeal.market` (no trailing slash) |
| `METABASE_USERNAME` | Your Metabase login email |
| `METABASE_PASSWORD` | Your Metabase login password |
| `GOOGLE_SHEET_ID` | The ID from your sheet's URL — the long string between `/d/` and `/edit` |
| `GOOGLE_SERVICE_ACCOUNT_B64` | The base64 string from Step 3 |

Secrets are encrypted and never shown again once saved — if you need to change one later, you overwrite it, you can't view the old value.

---

## Step 5 — Push and test

1. Commit and push the three files if you haven't already (Step 1).
2. In GitHub, go to the **Actions** tab of your repo. You should see the workflow **"Hourly Merchant Dashboard ETL"** listed (GitHub picks it up automatically from `.github/workflows/`).
3. Click into it → **Run workflow** (top right) → **Run workflow** again to confirm. This triggers an immediate manual run without waiting for the next hour.
4. Click the running job to watch logs live. A successful run ends with a log line like `Done — N summarized rows pushed for 2026-07.`
5. Open your Google Sheet — you should see a `Hourly_Merchant_Summary` tab (and a `Merchant_Monthly_State` tab) populated.

If it fails, the most common causes are: a typo in a secret name, the service account not having Editor access on the sheet, or the Metabase username/password being wrong — the log output will say which step failed.

---

## Step 6 — Let it run automatically

Once the manual test succeeds, no further action is needed — the `schedule: cron: "0 * * * *"` line in the workflow file runs it every hour, at the top of the hour, in UTC (GitHub Actions cron always runs in UTC regardless of your timezone; the script itself converts everything to IST internally, so this doesn't affect the data).

Two GitHub-specific quirks worth knowing:
- Scheduled runs are **best-effort** — GitHub may delay a run by several minutes during high load. This doesn't lose data since each run recomputes the full month-to-date summary.
- GitHub **disables scheduled workflows automatically after 60 days of repo inactivity** (no commits/pushes). If that happens, go to the Actions tab and re-enable it, or just push a commit periodically.

---

## Ongoing maintenance

- If your Metabase password changes, update the `METABASE_PASSWORD` secret.
- If card 444 or 445 change their column names, the script will raise a clear `KeyError` naming exactly which column is missing — fix it in the `COLUMN_ALIASES` dictionary near the top of the script rather than the logic below it.
- Retention/Reactivation numbers will read as 0 for the first month after you turn this on — they need one full month of `Merchant_Monthly_State` history to compare against.
