{{ config (
    alias = target.database + '_blended_performance'
)}}
    
WITH initial_sho_data as
    (SELECT day::date as date, 'day' as date_granularity, order_id, customer_order_index, gross_revenue 
        FROM {{ source('reporting','shopify_daily_sales_by_order') }} 
    UNION ALL
    SELECT week::date as date, 'week' as date_granularity, order_id, customer_order_index, gross_revenue 
        FROM {{ source('reporting','shopify_daily_sales_by_order') }} 
    UNION ALL
    SELECT month::date as date, 'month' as date_granularity, order_id, customer_order_index, gross_revenue 
        FROM {{ source('reporting','shopify_daily_sales_by_order') }} 
    UNION ALL
    SELECT quarter::date as date, 'quarter' as date_granularity, order_id, customer_order_index, gross_revenue 
        FROM {{ source('reporting','shopify_daily_sales_by_order') }} 
    UNION ALL
    SELECT year::date as date, 'year' as date_granularity, order_id, customer_order_index, gross_revenue 
        FROM {{ source('reporting','shopify_daily_sales_by_order') }}),
    
    paid_data as
    (SELECT channel, date::date, date_granularity, campaign_name, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(acq_spend),0) as acq_spend, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(purchases),0) as paid_purchases, 
        COALESCE(SUM(acq_purchases),0) as paid_acq_purchases, COALESCE(SUM(revenue),0) as paid_revenue, 
        0 as sho_purchases, 0 as sho_revenue, 0 as sho_first_orders, 0 as sho_first_order_revenue, 0 as sho_repeat_orders, 0 as sho_repeat_order_revenue, 0 as sho_net_revenue
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, campaign_name,
            spend, CASE WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Ret' THEN spend ELSE 0 END as acq_spend,
            impressions, link_clicks as clicks, add_to_cart, 
            purchases, CASE WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Ret' THEN purchases ELSE 0 END as acq_purchases, revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, campaign_name,
            spend, CASE WHEN campaign_name ~* 'PMax' THEN spend ELSE 0 END as acq_spend,
            impressions, clicks, null as add_to_cart, 
            purchases, CASE WHEN campaign_name ~* 'PMax' THEN purchases ELSE 0 END as acq_purchases, revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'TikTok' as channel, date, date_granularity, campaign_name,
            spend, spend as acq_spend, impressions, clicks, null as add_to_cart, purchases, purchases as acq_purchases, revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        )
    GROUP BY channel, date, date_granularity, campaign_name),

    shopify_data as
    ( 
    SELECT 'Shopify' as channel, date, date_granularity, null as campaign_name, 
        0 as spend, 0 as acq_spend, 0 as impressions, 0 as clicks, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_acq_purchases, 0 as paid_revenue,
        COUNT(DISTINCT order_id) as sho_purchases, COALESCE(SUM(gross_revenue),0) as sho_revenue, 
        COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id ELSE 0 END) as sho_first_orders, 
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue ELSE 0 END),0) as sho_first_order_revenue, 
        COUNT(DISTINCT CASE WHEN customer_order_index > 1 THEN order_id ELSE 0 END) as sho_repeat_orders, 
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue ELSE 0 END),0) as sho_repeat_order_revenue,
        0 as sho_net_revenue
    FROM initial_sho_data
    GROUP BY channel, date, date_granularity, campaign_name
    UNION ALL
    SELECT 'Shopify' as channel, date_granularity, date, null as campaign_name, 
        0 as spend, 0 as acq_spend, 0 as impressions, 0 as clicks, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_acq_purchases, 0 as paid_revenue,
        0 as sho_purchases, 0 as sho_revenue, 0 as sho_first_orders, 0 as sho_first_order_revenue, 0 as sho_repeat_orders, 0 as sho_repeat_order_revenue,
        COALESCE(SUM(net_sales),0) as sho_net_revenue
    FROM {{ source('reporting','shopify_sales') }}
    GROUP BY channel, date, date_granularity, campaign_name)
    
SELECT * FROM paid_data
UNION ALL
SELECT * FROM shopify_data
