# Marketing Analytics Pipeline

Daily Airflow pipeline pulling Google Ads and Facebook Ads performance data into BigQuery, then transforming with dbt into a unified campaign performance mart with ROAS, CAC, CPM, and CTR metrics.

## Architecture

```
Google Ads API          Facebook Marketing API
      │                        │
      ▼                        ▼
  google_ads.py           facebook_ads.py
      │                        │
      └──────── Airflow DAG ───┘
                    │  (06:00 UTC daily)
                    ▼
              BigQuery raw tables
              marketing_raw.google_ads_campaigns
              marketing_raw.facebook_ads_campaigns
                    │
                    ▼
               dbt models
               stg_google_ads → stg_facebook_ads
                    │
                    ▼
          mart_campaign_performance
          (unified ROAS · CAC · CTR · CPM)
```

## Key metrics

| Metric | Formula |
|---|---|
| ROAS | `conversion_value / spend` |
| CAC | `spend / conversions` |
| CTR | `clicks / impressions` |
| CPM | `spend × 1000 / impressions` |
| 7-day rolling ROAS | window function per channel × campaign |

## Quick Start

```bash
pip install google-ads facebook-business apache-airflow google-cloud-bigquery pandas

# Set credentials
export GOOGLE_ADS_DEVELOPER_TOKEN="..."
export GOOGLE_ADS_CLIENT_ID="..."
export GOOGLE_ADS_CLIENT_SECRET="..."
export GOOGLE_ADS_REFRESH_TOKEN="..."
export GOOGLE_ADS_CUSTOMER_ID="1234567890"
export META_APP_ID="..."
export META_APP_SECRET="..."
export META_ACCESS_TOKEN="..."
export META_AD_ACCOUNT_ID="act_1234567890"
export GCP_PROJECT="my-project"

# Run manually for a date
python -c "
from connectors.google_ads import GoogleAdsConnector
from datetime import date
df = GoogleAdsConnector().get_campaign_performance(date(2026,7,1), date(2026,7,1))
print(df[['campaign_name','spend_usd','roas','ctr']].head())
"

# Or trigger via Airflow
airflow dags trigger marketing_analytics_pipeline --conf '{"ds":"2026-07-01"}'
```

## Extending

Add a new ad platform by creating a connector in `connectors/` that returns a DataFrame with columns `date, channel, campaign_name, impressions, clicks, cost_usd, conversions, revenue`, then add a corresponding `stg_*.sql` model and append a UNION ALL branch in `mart_campaign_performance.sql`.
