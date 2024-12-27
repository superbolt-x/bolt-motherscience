{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH paid_data as
    (SELECT channel, date::date, date_granularity, campaign_name, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(purchases),0) as paid_purchases, COALESCE(SUM(revenue),0) as paid_revenue, 
        0 as sho_purchases, 0 as sho_revenue, 0 as sho_first_orders, 0 as sho_first_order_revenue, 0 as sho_repeat_orders, 0 as sho_repeat_order_revenue
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, campaign_name,
            spend, impressions, link_clicks as clicks, add_to_cart, purchases, revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity, campaign_name,
            spend, impressions, clicks, null as add_to_cart, purchases, revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        )
    GROUP BY channel, date, date_granularity, campaign_name),

    shopify_data as
    (SELECT 'Shopify' as channel, date, date_granularity, null as campaign_name, 
        0 as spend, 0 as impressions, 0 as clicks, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue,
        COALESCE(SUM(orders),0) as sho_purchases, COALESCE(SUM(gross_sales),0) as sho_revenue, 
        COALESCE(SUM(first_orders),0) as sho_first_orders, COALESCE(SUM(first_order_gross_sales),0) as sho_first_order_revenue, 
        COALESCE(SUM(repeat_orders),0) as sho_repeat_orders, COALESCE(SUM(repeat_order_gross_sales),0) as sho_repeat_order_revenue
    FROM {{ source('reporting','shopify_sales') }}
    GROUP BY channel, date, date_granularity, campaign_name)
    
SELECT * FROM paid_data
UNION ALL
SELECT * FROM shopify_data
