/*
Usefull filtering, can be used in IF, CASE statments, SUM() OVER functions
  CASE
    WHEN event_name ='session_start' THEN 1
    WHEN event_name ='first_visit' THEN 2
    WHEN event_name ='page_view' THEN 3
    ELSE 4
  END
*/
WITH smaller_table AS(
  SELECT
  CASE
    WHEN
      IFNULL(TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)), SECOND), 0) >37*60
      OR event_name = 'session_start' THEN 0
    ELSE
      IFNULL(TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)), SECOND), 0)
  END diff_sec,
  CASE
    WHEN event_name = 'session_start' THEN 1
    WHEN LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp, (CASE WHEN event_name ='session_start' THEN 1 WHEN event_name ='first_visit' THEN 2 WHEN event_name ='page_view' THEN 3 ELSE 4 END)) IS NULL THEN 1
    WHEN IFNULL(TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)), SECOND), 0) >37*60 THEN 1
    ELSE 0
  END Is_session_start,
  #LAG(event_timestamp) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp, (CASE WHEN event_name ='session_start' THEN 1 WHEN event_name ='first_visit' THEN 2 WHEN event_name ='page_view' THEN 3 ELSE 4 END)) previous_session_time,
    PARSE_DATE('%Y%m%d', rw.event_date) AS event_date,
    #EXTRACT(TIME FROM TIMESTAMP_MICROS(rw.event_timestamp)) event_time,
    #DENSE_RANK() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) rank_click,
    rw.event_timestamp,
    rw.event_name,
    rw.user_pseudo_id,
    rw.country,
    rw.campaign,
    rw.page_title,
    rw.transaction_id,
    rw.traffic_source,
   /* rw.category,
    rw.mobile_brand_name,
    rw.operating_system,
    rw.browser,
    rw.browser_version, */
    rw.total_item_quantity,
    rw.purchase_revenue_in_usd,
  FROM
    `tc-da-1.turing_data_analytics.raw_events` rw
),
#users then session started with different event
sessions_starts AS(
SELECT
  user_pseudo_id,
  event_timestamp,
  ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) session_per_user,
FROM smaller_table
WHERE Is_session_start = 1
#ORDER BY user_pseudo_id, rank_click, (CASE WHEN event_name ='session_start' THEN 1 WHEN event_name ='first_visit' THEN 2 WHEN event_name ='page_view' THEN 3 ELSE 4 END)
),
adding_session_id AS(
SELECT
  sm.user_pseudo_id,
  sm.event_timestamp,
  sm.event_name,
  sm.Is_session_start,
  start.session_per_user session_no,
  #start.event_timestamp start_time,
  # should be 4 295 584 rows at the end
  IF(MAX(start.event_timestamp) OVER (PARTITION BY sm.user_pseudo_id, sm.event_timestamp, sm.event_name) = start.event_timestamp, 1, 0) right_join,
  CONCAT(RIGHT(sm.user_pseudo_id, 15), LEFT(CAST(start.event_timestamp AS STRING), 16)) session_id,
FROM smaller_table sm
JOIN sessions_starts AS start
 ON sm.user_pseudo_id = start.user_pseudo_id
 WHERE sm.event_timestamp >= start.event_timestamp
),
#total 360 464 sessions started, unique time 2 902 279
# should be 4 295 584 rows at the end
Detailed_table AS(
SELECT
  si.session_id,
  si.session_no,
  #si.start_time,
  st.* EXCEPT(total_item_quantity , transaction_id),
  #COUNT(DISTINCT st.event_timestamp) OVER (PARTITION BY si.session_id) session_clicks,
  #COUNTIF(st.event_name NOT IN ('first_visit','session_start','user_engagement')) OVER (PARTITION BY si.session_id) session_clicks_mod,
    CASE
    WHEN purchase_revenue_in_usd IS NOT NULL AND purchase_revenue_in_usd != 0 THEN total_item_quantity
    ELSE NULL
  END purchased_item_quantity,
  CASE
    WHEN transaction_id IS NOT NULL AND transaction_id NOT LIKE '(not_set)' THEN transaction_id
    ELSE NULL
  END transaction_ids,
  CASE
    WHEN campaign IN ('BlackFriday_V1', 'BlackFriday_V2', 'Holiday_V1', 'Holiday_V2', 'NewYear_V1', 'NewYear_V2', 'Data Share Promo', '(referral)', '(organic)', '(direct)', '(data deleted)', '<Other>') THEN campaign
    ELSE NULL
  END marketing_campaign,
  CASE WHEN campaign IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY st.user_pseudo_id, session_id, (CASE WHEN campaign IS NOT NULL THEN 1 ELSE 0 END) ORDER BY st.event_timestamp, st.event_name) END camp_rank,
  CASE WHEN traffic_source IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY st.user_pseudo_id, session_id, (CASE WHEN traffic_source IS NOT NULL THEN 1 ELSE 0 END) ORDER BY st.event_timestamp, st.event_name) END traffic_rank
FROM smaller_table st
JOIN adding_session_id si
ON st.user_pseudo_id = si.user_pseudo_id AND st.event_timestamp = si.event_timestamp AND st.event_name = si.event_name WHERE si.right_join = 1)
SELECT
  session_id,
  session_no,
  user_pseudo_id,
  country,
  SUM(diff_sec) duration_sec,
  MIN(event_date) session_start,
  MAX(event_date) session_end,
  EXTRACT(TIME FROM TIMESTAMP_MICROS(MIN(event_timestamp))) session_start_time,
  EXTRACT(TIME FROM TIMESTAMP_MICROS(MAX(event_timestamp))) session_end_time,
  MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) is_first_visit,
  #MAX(CASE WHEN marketing_campaign IS NOT NULL THEN marketing_campaign ELSE NULL END) marketing_campaign,
  MAX(CASE WHEN Is_session_start = 1 THEN page_title ELSE NULL END) first_page,
  MAX(CASE WHEN camp_rank = 1 THEN marketing_campaign ELSE NULL END) marketing_campaign,
  MAX(CASE WHEN traffic_rank = 1 THEN traffic_source ELSE NULL END) traffic_source,
  COUNT(DISTINCT page_title) page_count,
  COUNT(DISTINCT event_timestamp) session_clicks,
  COUNTIF(event_name NOT IN ('first_visit','session_start','user_engagement')) session_clicks_mod,
  COUNT(DISTINCT transaction_ids) tranaction_count,
  SUM(purchased_item_quantity) item_count,
  SUM(purchase_revenue_in_usd) revenue
FROM Detailed_table
GROUP BY session_id, session_no, user_pseudo_id, country
--
# there are 450 purchases with 0 revenue, null items, (not set tansaction_id)
# transaction_id has null and (not set) and then number and one session can have more than one transaction
# can be included first page title per session
--
#ORDER BY st.user_pseudo_id, st.event_timestamp #, (CASE WHEN st.event_name ='session_start' THEN 1 WHEN st.event_name ='first_visit' THEN 2 WHEN st.event_name ='page_view' THEN 3 ELSE 4 END)
