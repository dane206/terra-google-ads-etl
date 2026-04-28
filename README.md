# terra-google-ads-etl

Pulls daily Google Ads performance data → `terra-analytics-prod.sources.*`

## Tables produced

| Table | Granularity |
|---|---|
| `google_ads_campaigns_daily` | campaign × day |
| `google_ads_adgroups_daily` | ad group × day |
| `google_ads_ads_daily` | ad × day |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add credentials to `config.ini`:

```ini
[google_ads]
developer_token = YOUR_DEVELOPER_TOKEN
client_id       = YOUR_CLIENT_ID
client_secret   = YOUR_CLIENT_SECRET
refresh_token   = YOUR_REFRESH_TOKEN
customer_id     = 1234567890
```

To generate a refresh token (one-time):

```bash
python google_ads_auth.py
```

## Usage

```bash
# Incremental — last 7 days, APPEND
python google_ads_to_bigquery.py --mode incremental

# Backfill — full history from a start date, TRUNCATE
python google_ads_to_bigquery.py --mode backfill --start 2024-04-01
```

## Auth

OAuth2 via Google Ads API. Run `google_ads_auth.py` once to generate a refresh token, then add it to `config.ini`. BigQuery uses Application Default Credentials (ADC).
