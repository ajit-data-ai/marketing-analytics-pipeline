"""
Google Ads API connector.

Pulls campaign, ad group, and keyword performance metrics for a given
date range and loads them into BigQuery. Uses the Google Ads API v17
(Customer report).

Required env vars:
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN
  GOOGLE_ADS_CUSTOMER_ID   (10-digit, no dashes)
"""
from __future__ import annotations
import os
from datetime import date
from typing import Iterator, List
import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


CAMPAIGN_QUERY = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        metrics.average_cpc
    FROM campaign
    WHERE segments.date BETWEEN '{start}' AND '{end}'
      AND campaign.status = 'ENABLED'
    ORDER BY segments.date DESC
"""

KEYWORD_QUERY = """
    SELECT
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        campaign.name,
        segments.date,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions
    FROM keyword_view
    WHERE segments.date BETWEEN '{start}' AND '{end}'
    ORDER BY metrics.cost_micros DESC
    LIMIT 500
"""


class GoogleAdsConnector:
    def __init__(self) -> None:
        self._client = GoogleAdsClient.load_from_dict({
            "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
            "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
            "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
            "use_proto_plus": True,
        })
        self._customer_id = os.environ["GOOGLE_ADS_CUSTOMER_ID"]

    def _run_query(self, query: str) -> List[dict]:
        service = self._client.get_service("GoogleAdsService")
        stream = service.search_stream(customer_id=self._customer_id, query=query)
        rows = []
        for batch in stream:
            for row in batch.results:
                rows.append(row)
        return rows

    def get_campaign_performance(self, start: date, end: date) -> pd.DataFrame:
        q = CAMPAIGN_QUERY.format(start=start.isoformat(), end=end.isoformat())
        raw = self._run_query(q)
        records = []
        for r in raw:
            records.append({
                "date": r.segments.date,
                "campaign_id": r.campaign.id,
                "campaign_name": r.campaign.name,
                "impressions": r.metrics.impressions,
                "clicks": r.metrics.clicks,
                "cost_usd": r.metrics.cost_micros / 1_000_000,
                "conversions": r.metrics.conversions,
                "conversion_value": r.metrics.conversions_value,
                "avg_cpc_usd": r.metrics.average_cpc / 1_000_000,
            })
        df = pd.DataFrame(records)
        df["ctr"] = df["clicks"] / df["impressions"].replace(0, float("nan"))
        df["roas"] = df["conversion_value"] / df["cost_usd"].replace(0, float("nan"))
        return df

    def get_keyword_performance(self, start: date, end: date) -> pd.DataFrame:
        q = KEYWORD_QUERY.format(start=start.isoformat(), end=end.isoformat())
        raw = self._run_query(q)
        records = []
        for r in raw:
            records.append({
                "date": r.segments.date,
                "keyword": r.ad_group_criterion.keyword.text,
                "match_type": r.ad_group_criterion.keyword.match_type.name,
                "campaign": r.campaign.name,
                "impressions": r.metrics.impressions,
                "clicks": r.metrics.clicks,
                "cost_usd": r.metrics.cost_micros / 1_000_000,
                "conversions": r.metrics.conversions,
            })
        return pd.DataFrame(records)
