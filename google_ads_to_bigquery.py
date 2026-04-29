#!/usr/bin/env python3
"""
Terra Health Essentials — Google Ads API → BigQuery ETL
========================================================
Pulls daily performance data at campaign / ad group / ad level.

Tables produced:
  terra-analytics-prod.sources.google_ads_campaigns_daily
  terra-analytics-prod.sources.google_ads_adgroups_daily
  terra-analytics-prod.sources.google_ads_ads_daily

Run modes:
  python google_ads_to_bigquery.py --mode backfill --start 2024-04-01
  python google_ads_to_bigquery.py --mode incremental   # last 7 days

Config (config.ini):
  [google_ads]
  developer_token = YOUR_DEVELOPER_TOKEN
  client_id       = YOUR_CLIENT_ID
  client_secret   = YOUR_CLIENT_SECRET
  refresh_token   = YOUR_REFRESH_TOKEN   # from google_ads_auth.py
  customer_id     = 1234567890            # no dashes

Requirements:
  pip install google-ads google-cloud-bigquery
"""

import os, sys, argparse, traceback
from datetime import datetime, timedelta, date

import configparser
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery

# ── Config — env vars override config.ini (Cloud Run uses env vars) ───────────
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

def _cfg(env_key, ini_section, ini_key):
    val = os.environ.get(env_key)
    if not val:
        try: val = config[ini_section][ini_key]
        except KeyError: pass
    if not val:
        print(f"❌ Missing config: {env_key} / [{ini_section}] {ini_key}")
        sys.exit(1)
    return val

DEVELOPER_TOKEN = _cfg("GOOGLE_ADS_DEVELOPER_TOKEN", "google_ads", "developer_token")
CLIENT_ID       = _cfg("GOOGLE_ADS_CLIENT_ID",       "google_ads", "client_id")
CLIENT_SECRET   = _cfg("GOOGLE_ADS_CLIENT_SECRET",   "google_ads", "client_secret")
REFRESH_TOKEN   = _cfg("GOOGLE_ADS_REFRESH_TOKEN",   "google_ads", "refresh_token")
CUSTOMER_ID     = _cfg("GOOGLE_ADS_CUSTOMER_ID",     "google_ads", "customer_id").replace("-", "")

BQ_PROJECT = os.environ.get("BQ_PROJECT", "terra-analytics-prod")
BQ_DATASET = "sources"

# ── Clients ───────────────────────────────────────────────────────────────────
google_ads_client = GoogleAdsClient.load_from_dict({
    "developer_token": DEVELOPER_TOKEN,
    "client_id":       CLIENT_ID,
    "client_secret":   CLIENT_SECRET,
    "refresh_token":   REFRESH_TOKEN,
    "use_proto_plus":  True,
})

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schemas ───────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField

CAMPAIGNS_SCHEMA = [
    SF("date",                       "DATE"),
    SF("campaign_id",                "STRING"),
    SF("campaign_name",              "STRING"),
    SF("campaign_status",            "STRING"),
    SF("campaign_type",              "STRING"),
    SF("impressions",                "INTEGER"),
    SF("clicks",                     "INTEGER"),
    SF("cost_micros",                "INTEGER"),
    SF("cost",                       "FLOAT"),
    SF("conversions",                "FLOAT"),
    SF("conversions_value",          "FLOAT"),
    SF("view_through_conversions",   "INTEGER"),
]

ADGROUPS_SCHEMA = [
    SF("date",                "DATE"),
    SF("campaign_id",         "STRING"),
    SF("campaign_name",       "STRING"),
    SF("ad_group_id",         "STRING"),
    SF("ad_group_name",       "STRING"),
    SF("ad_group_status",     "STRING"),
    SF("impressions",         "INTEGER"),
    SF("clicks",              "INTEGER"),
    SF("cost_micros",         "INTEGER"),
    SF("cost",                "FLOAT"),
    SF("conversions",         "FLOAT"),
    SF("conversions_value",   "FLOAT"),
]

ADS_SCHEMA = [
    SF("date",                "DATE"),
    SF("campaign_id",         "STRING"),
    SF("campaign_name",       "STRING"),
    SF("ad_group_id",         "STRING"),
    SF("ad_group_name",       "STRING"),
    SF("ad_id",               "STRING"),
    SF("ad_name",             "STRING"),
    SF("ad_type",             "STRING"),
    SF("final_urls",          "STRING"),
    SF("impressions",         "INTEGER"),
    SF("clicks",              "INTEGER"),
    SF("cost_micros",         "INTEGER"),
    SF("cost",                "FLOAT"),
    SF("conversions",         "FLOAT"),
    SF("conversions_value",   "FLOAT"),
]

# ── GAQL Queries ──────────────────────────────────────────────────────────────
def campaigns_query(start, end):
    return f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.view_through_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        AND campaign.status != 'REMOVED'
        ORDER BY segments.date DESC
    """

def adgroups_query(start, end):
    return f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group.status,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        AND ad_group.status != 'REMOVED'
        ORDER BY segments.date DESC
    """

def ads_query(start, end):
    return f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            ad_group_ad.ad.type,
            ad_group_ad.ad.final_urls,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        AND ad_group_ad.status != 'REMOVED'
        ORDER BY segments.date DESC
    """

# ── Runner ────────────────────────────────────────────────────────────────────
def run_query(gaql):
    service = google_ads_client.get_service("GoogleAdsService")
    request = google_ads_client.get_type("SearchGoogleAdsStreamRequest")
    request.customer_id = CUSTOMER_ID
    request.query = gaql
    return service.search_stream(request)

def parse_campaigns(response):
    rows = []
    for batch in response:
        for row in batch.results:
            m = row.metrics
            c = row.campaign
            rows.append({
                "date":                     str(row.segments.date),
                "campaign_id":              str(c.id),
                "campaign_name":            c.name,
                "campaign_status":          c.status.name,
                "campaign_type":            c.advertising_channel_type.name,
                "impressions":              m.impressions,
                "clicks":                   m.clicks,
                "cost_micros":              m.cost_micros,
                "cost":                     round(m.cost_micros / 1_000_000, 4),
                "conversions":              m.conversions,
                "conversions_value":        m.conversions_value,
                "view_through_conversions": m.view_through_conversions,
            })
    return rows

def parse_adgroups(response):
    rows = []
    for batch in response:
        for row in batch.results:
            m  = row.metrics
            c  = row.campaign
            ag = row.ad_group
            rows.append({
                "date":              str(row.segments.date),
                "campaign_id":       str(c.id),
                "campaign_name":     c.name,
                "ad_group_id":       str(ag.id),
                "ad_group_name":     ag.name,
                "ad_group_status":   ag.status.name,
                "impressions":       m.impressions,
                "clicks":            m.clicks,
                "cost_micros":       m.cost_micros,
                "cost":              round(m.cost_micros / 1_000_000, 4),
                "conversions":       m.conversions,
                "conversions_value": m.conversions_value,
            })
    return rows

def parse_ads(response):
    rows = []
    for batch in response:
        for row in batch.results:
            m  = row.metrics
            c  = row.campaign
            ag = row.ad_group
            ad = row.ad_group_ad.ad
            rows.append({
                "date":              str(row.segments.date),
                "campaign_id":       str(c.id),
                "campaign_name":     c.name,
                "ad_group_id":       str(ag.id),
                "ad_group_name":     ag.name,
                "ad_id":             str(ad.id),
                "ad_name":           ad.name or None,
                "ad_type":           ad.type_.name,
                "final_urls":        ad.final_urls[0] if ad.final_urls else None,
                "impressions":       m.impressions,
                "clicks":            m.clicks,
                "cost_micros":       m.cost_micros,
                "cost":              round(m.cost_micros / 1_000_000, 4),
                "conversions":       m.conversions,
                "conversions_value": m.conversions_value,
            })
    return rows

def load_to_bq(table, rows, schema, mode):
    if not rows:
        print(f"  ⚠️  {table} — no rows")
        return
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=mode,
        ignore_unknown_values=True,
    ))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

# ── Date helpers ──────────────────────────────────────────────────────────────
def get_max_date(table):
    """Return MAX(date) from a BQ table as a string, or None if table is empty."""
    try:
        result = list(bq.query(f"SELECT CAST(MAX(date) AS STRING) AS max_date FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`").result())
        val = result[0].max_date if result else None
        if val:
            print(f"  Resuming from last date in {table}: {val}")
        return val
    except Exception:
        return None

def date_chunks(start_str, end_str, chunk_days=90):
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    while start <= end:
        chunk_end = min(start + timedelta(days=chunk_days - 1), end)
        yield str(start), str(chunk_end)
        start = chunk_end + timedelta(days=1)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--start", default="2024-04-01", help="Start date for backfill (YYYY-MM-DD)")
    args = parser.parse_args()

    end_date = str(date.today() - timedelta(days=1))

    if args.mode == "incremental":
        # Resume from MAX(date) in BQ, with 2-day overlap for late-arriving conversion data
        max_date = get_max_date("google_ads_campaigns_daily")
        if max_date:
            start_date = str((datetime.strptime(max_date, "%Y-%m-%d").date() - timedelta(days=2)))
        else:
            start_date = str(date.today() - timedelta(days=7))
        bq_mode = bigquery.WriteDisposition.WRITE_APPEND
        print(f"🚀 Google Ads incremental: {start_date} → {end_date}")
    else:
        start_date = args.start
        bq_mode = bigquery.WriteDisposition.WRITE_TRUNCATE
        print(f"🚀 Google Ads backfill: {start_date} → {end_date}")

    all_campaigns, all_adgroups, all_ads = [], [], []

    for chunk_start, chunk_end in date_chunks(start_date, end_date):
        print(f"  Fetching {chunk_start} → {chunk_end}...")
        try:
            all_campaigns += parse_campaigns(run_query(campaigns_query(chunk_start, chunk_end)))
            all_adgroups  += parse_adgroups(run_query(adgroups_query(chunk_start, chunk_end)))
            all_ads       += parse_ads(run_query(ads_query(chunk_start, chunk_end)))
        except GoogleAdsException as e:
            print(f"  ❌ Google Ads API error: {e}")
            raise

    print(f"\n  campaigns: {len(all_campaigns):,} | adgroups: {len(all_adgroups):,} | ads: {len(all_ads):,}")
    print("\n💾 Loading to BigQuery...")
    load_to_bq("google_ads_campaigns_daily", all_campaigns, CAMPAIGNS_SCHEMA, bq_mode)
    load_to_bq("google_ads_adgroups_daily",  all_adgroups,  ADGROUPS_SCHEMA,  bq_mode)
    load_to_bq("google_ads_ads_daily",       all_ads,       ADS_SCHEMA,       bq_mode)

    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
