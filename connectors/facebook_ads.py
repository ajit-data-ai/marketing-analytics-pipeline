"""
Facebook / Meta Ads Marketing API connector.

Pulls campaign, ad set, and ad-level performance metrics using the
facebook-business SDK. Handles pagination and rate limiting.

Required env vars:
  META_APP_ID
  META_APP_SECRET
  META_ACCESS_TOKEN   (long-lived system user token)
  META_AD_ACCOUNT_ID  (act_XXXXXXXXXXXXXXXX)
"""
from __future__ import annotations
import os
from datetime import date
from typing import List
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights


CAMPAIGN_FIELDS = [
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.date_start,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.reach,
    AdsInsights.Field.frequency,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cpc,
    AdsInsights.Field.ctr,
    AdsInsights.Field.conversions,
    AdsInsights.Field.purchase_roas,
]

ADSET_FIELDS = [
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.date_start,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.conversions,
]


class FacebookAdsConnector:
    def __init__(self) -> None:
        FacebookAdsApi.init(
            app_id=os.environ["META_APP_ID"],
            app_secret=os.environ["META_APP_SECRET"],
            access_token=os.environ["META_ACCESS_TOKEN"],
        )
        self._account = AdAccount(os.environ["META_AD_ACCOUNT_ID"])

    def _fetch_insights(self, fields: List[str], start: date, end: date, level: str) -> pd.DataFrame:
        params = {
            "level": level,
            "time_range": {"since": start.isoformat(), "until": end.isoformat()},
            "time_increment": 1,
            "limit": 500,
        }
        cursor = self._account.get_insights(fields=fields, params=params)
        records = []
        for row in cursor:
            records.append(dict(row))
        return pd.DataFrame(records)

    def get_campaign_performance(self, start: date, end: date) -> pd.DataFrame:
        df = self._fetch_insights(CAMPAIGN_FIELDS, start, end, level="campaign")
        df["spend"] = pd.to_numeric(df.get("spend", 0), errors="coerce")
        df["impressions"] = pd.to_numeric(df.get("impressions", 0), errors="coerce")
        df["clicks"] = pd.to_numeric(df.get("clicks", 0), errors="coerce")
        df["roas"] = df.get("purchase_roas", pd.Series(dtype=float)).apply(
            lambda x: float(x[0]["value"]) if isinstance(x, list) and x else None
        )
        return df.rename(columns={"date_start": "date", "spend": "cost_usd"})

    def get_adset_performance(self, start: date, end: date) -> pd.DataFrame:
        return self._fetch_insights(ADSET_FIELDS, start, end, level="adset")
