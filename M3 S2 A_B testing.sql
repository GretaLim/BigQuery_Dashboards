WITH raw_event AS(
  SELECT
    CAST(CONCAT(LEFT(event_date, 4),RIGHT(LEFT(event_date,6),2)) aS INT64) Month,
    campaign,
    event_name,
    COUNT(event_name) events_count,
    count(DISTINCT user_pseudo_id) users_count
  FROM
    `tc-da-1.turing_data_analytics.raw_events`
  WHERE (campaign LIKE '%NewYear%' AND CAST(CONCAT(LEFT(event_date, 4),RIGHT(LEFT(event_date,6),2)) aS INT64) = 202101
   OR campaign LIKE '%BlackFriday%' AND CAST(CONCAT(LEFT(event_date, 4),RIGHT(LEFT(event_date,6),2)) aS INT64) = 202011) AND event_name = 'page_view'
  GROUP BY campaign, event_name, Month
  ORDER BY 1, 2)

SELECT
  adsence.*,
  event.users_count,
  ROUND(adsence.Clicks/adsence.Impressions*100, 2) Adsence_conversion,
  ROUND(event.users_count/adsence.Impressions*100, 2) Events_conversion
FROM
  `tc-da-1.turing_data_analytics.adsense_monthly` AS adsence
LEFT JOIN raw_event event
ON adsence.Campaign = event.campaign
WHERE (adsence.Campaign LIKE '%NewYear%' OR adsence.Campaign LIKE '%BlackFriday%' ) and adsence.Month != 202111
