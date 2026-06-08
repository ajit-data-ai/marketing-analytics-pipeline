-- Unified campaign performance mart joining Google Ads and Facebook Ads.
-- One row per channel × campaign × date.
-- Key metrics: ROAS, CAC, CPM, CTR.

WITH google_ads AS (
    SELECT
        date,
        'google'            AS channel,
        campaign_name,
        impressions,
        clicks,
        cost_usd,
        conversions,
        conversion_value    AS revenue,
        avg_cpc_usd         AS cpc,
        ctr
    FROM {{ ref('stg_google_ads') }}
),

facebook_ads AS (
    SELECT
        date,
        'facebook'          AS channel,
        campaign_name,
        CAST(impressions AS INT64)    AS impressions,
        CAST(clicks AS INT64)         AS clicks,
        cost_usd,
        CAST(conversions AS FLOAT64)  AS conversions,
        CAST(roas AS FLOAT64) * cost_usd AS revenue,
        CAST(cpc AS FLOAT64)          AS cpc,
        CAST(ctr AS FLOAT64) / 100    AS ctr
    FROM {{ ref('stg_facebook_ads') }}
),

combined AS (
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM facebook_ads
),

with_metrics AS (
    SELECT
        date,
        channel,
        campaign_name,
        impressions,
        clicks,
        ROUND(cost_usd, 2)                                  AS spend_usd,
        CAST(conversions AS INT64)                          AS conversions,
        ROUND(revenue, 2)                                   AS revenue_usd,
        ROUND(cost_usd / NULLIF(conversions, 0), 2)         AS cac_usd,
        ROUND(revenue / NULLIF(cost_usd, 0), 3)             AS roas,
        ROUND(ctr * 100, 3)                                 AS ctr_pct,
        ROUND(cost_usd * 1000 / NULLIF(impressions, 0), 2)  AS cpm_usd,
        ROUND(cpc, 3)                                       AS cpc_usd
    FROM combined
)

SELECT
    *,
    -- 7-day rolling ROAS per channel × campaign
    ROUND(
        AVG(roas) OVER (
            PARTITION BY channel, campaign_name
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 3
    ) AS roas_7d_avg
FROM with_metrics
ORDER BY date DESC, spend_usd DESC
