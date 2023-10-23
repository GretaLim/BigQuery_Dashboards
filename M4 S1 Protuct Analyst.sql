WITH purchased_users AS(
  SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    DENSE_RANK() OVER (PARTITION BY user_pseudo_id, event_date ORDER BY FORMAT_TIME('%X',EXTRACT(TIME FROM TIMESTAMP_MICROS(event_timestamp)))) purchase_per_day_rank
  FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase' AND total_item_quantity IS NOT NULL),
smaller_table AS(
  SELECT
    #RANK() over (PARTITION BY rw.user_pseudo_id ORDER BY event_timestamp) rank_by_time,
    RANK() over (PARTITION BY rw.user_pseudo_id, rw.event_date ORDER BY event_timestamp) rank_by_time_day,
    #RANK() over (PARTITION BY rw.user_pseudo_id, rw.event_date, rw.event_name ORDER BY event_timestamp) rank_by_time_day_event,
    PARSE_DATE('%Y%m%d', rw.event_date) AS event_date,
   #FORMAT_DATETIME('%F %X',EXTRACT(DATETIME FROM TIMESTAMP_MICROS(rw.event_timestamp))) event_time_test,
    EXTRACT(TIME FROM TIMESTAMP_MICROS(rw.event_timestamp)) event_time,
    rw.event_timestamp,
    rw.event_name,
    rw.user_pseudo_id,
    rw.category,
    rw.mobile_brand_name,
    rw.operating_system,
    rw.browser,
    rw.browser_version,
    rw.total_item_quantity,
    rw.purchase_revenue_in_usd,
    rw.country,
    rw.transaction_id
  FROM
    `tc-da-1.turing_data_analytics.raw_events` rw
  WHERE rw.user_pseudo_id IN (SELECT DISTINCT(user_pseudo_id) FROM purchased_users) OR rw.total_item_quantity IS NOT NULL
),
start_points AS(
SELECT
  MIN(event_timestamp),
  event_date,
  event_time,
  user_pseudo_id
FROM smaller_table
  WHERE rank_by_time_day = 1 AND event_name IN ('session_start', 'page_view') #'first_visit', 'view_promotion', 'user_engagement') #
GROUP BY 2, 3, 4),
#rasti max session start, bet mazesne uz purchases date, arba MIN pageview kiekvienam useriui, kiekviena diena
test_start_point AS(
SELECT
  pu.*,
  FORMAT_TIME('%X', EXTRACT(TIME FROM TIMESTAMP_MICROS(pu.event_timestamp))) purchase_time,
  MAX(IF(st.event_name = 'session_start', st.event_time, NULL)) session_start,
  MIN(IF(st.event_name = 'page_view', st.event_time,NULL)) page_view,
FROM purchased_users AS pu
JOIN smaller_table AS st
  ON pu.user_pseudo_id = st.user_pseudo_id
    AND pu.event_date = st.event_date
    AND st.event_name IN ('session_start', 'page_view')
    AND st.event_timestamp < pu.event_timestamp
GROUP BY 1, 2, 3, 4 , 5 #,st.event_name
),
prefinal AS(
SELECT
  DENSE_RANK() OVER (PARTITION BY st.user_pseudo_id, st.event_date ORDER BY FORMAT_TIME('%X',st.event_time)) purchase_per_day_rank,
  #sp.purchase_time start_time,
  IF(sp.session_start IS NULL, page_view, session_start) start_time,
  TIME_DIFF(st.event_time, IF(sp.session_start IS NULL, page_view, session_start), MINUTE) duration_min,
  st.* EXCEPT (event_timestamp, rank_by_time_day)
FROM smaller_table  AS st
#JOIN start_points AS sp
#  ON sp.user_pseudo_id = st.user_pseudo_id AND sp.event_date = st.event_date
JOIN test_start_point sp
  ON sp.user_pseudo_id = st.user_pseudo_id AND sp.event_date = st.event_date AND sp.event_timestamp = st.event_timestamp
WHERE st.event_name = 'purchase' AND st.total_item_quantity IS NOT NULL)
SELECT
  prefinal.*,
  CASE
    WHEN duration_min <= duration_group.percentiles[offset(20)] THEN CONCAT('Up to 0',duration_group.percentiles[offset(20)], ' min')
    WHEN duration_min <= duration_group.percentiles[offset(40)] AND duration_min > duration_group.percentiles[offset(20)] THEN CONCAT('Up to ',duration_group.percentiles[offset(40)], ' min')
    WHEN duration_min <= duration_group.percentiles[offset(60)] AND duration_min > duration_group.percentiles[offset(40)] THEN CONCAT('Up to ',duration_group.percentiles[offset(60)], ' min')
    WHEN duration_min <= duration_group.percentiles[offset(80)] AND duration_min > duration_group.percentiles[offset(60)] THEN CONCAT('Up to ',duration_group.percentiles[offset(80)], ' min')
    WHEN duration_min <= duration_group.percentiles[offset(100)] AND duration_min > duration_group.percentiles[offset(80)] THEN CONCAT('Up to_ ',duration_group.percentiles[offset(100)], ' min')
  END duration_percentile,
FROM prefinal, (SELECT APPROX_QUANTILES(duration_min, 100) percentiles FROM prefinal) duration_group
ORDER BY user_pseudo_id, event_date, event_time
