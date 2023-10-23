-- changes in table raw_events: changed date_format, excluded columns without data or with the same one atribute to all observations
WITH date_format AS(
SELECT
--*
CAST(CONCAT(LEFT(event_date, 4),'-',RIGHT(LEFT(event_date,6),2), '-',RIGHT(event_date,2)) AS DATE) event_date_f,
event_timestamp,
event_name,
user_pseudo_id,
user_first_touch_timestamp,
country
FROM
`tc-da-1.turing_data_analytics.raw_events`
),
-- to see what is date range in tables records
Date_range AS(
SELECT
MIN(event_date_f) begining,
MAX(event_date_f) finish
FROM date_format),
--
--List of users, who don't have first_touch_timestamp
NO_first_touch AS(
SELECT
  *
FROM date_format
WHERE user_first_touch_timestamp IS NULL
),
-- CTE to calculate started sessions per user
session_start AS(
  SELECT
    DISTINCT(user_pseudo_id),
    SUM(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END) sessions_started
  FROM date_format
  GROUP BY user_pseudo_id
),
-- users distribution by started sessions
user_no_by_sessions AS(
SELECT
  DISTINCT sessions_started,
  COUNT(user_pseudo_id) users
  --COUNT(DISTINCT user_pseudo_id)
FROM session_start
GROUP BY sessions_started),
-- every user has assigned all events to one country
-- ranked events by event name and unique user id
ranked_events AS (
  SELECT
  --COUNT(DISTINCT user_pseudo_id)
  ROW_NUMBER() over (PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) rank_by_id_event,
  *
FROM date_format
WHERE event_name NOT IN ('first_visit', 'session_start')
),
ranked_events_adapted AS (
  SELECT
  --COUNT(DISTINCT user_pseudo_id)
  ROW_NUMBER() over (PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) rank_by_id_event,
  *
FROM date_format
WHERE event_name IN ('page_view', 'scroll', 'view_item','select_item', 'add_to_cart', 'begin_checkout', 'purchase')
ORDER BY user_pseudo_id, event_timestamp),
-- rank by event in identical user
steps AS(
SELECT
  CASE
    WHEN event_name = 'page_view' THEN 'Page view'
    WHEN event_name = 'scroll' THEN 'Scroll'
    WHEN event_name = 'view_item' THEN 'Interest in the item'
    WHEN event_name = 'select_item' OR event_name = 'add_to_cart' THEN 'Choise of the item'
    WHEN event_name = 'begin_checkout' THEN 'Begin the checkout'
    WHEN event_name = 'purchase' THEN 'Purchase'
  ELSE NULL
  END AS funnel_step,
  ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) rank_by_step,
  *
FROM(
SELECT
  --ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) rank_by_event,
  * EXCEPT(rank_by_id_event)
FROM ranked_events_adapted
WHERE rank_by_id_event = 1)),
-- CTE about how much users counted to every event_name and rank
event_rank_users AS(
SELECT
  DISTINCT funnel_step,
  rank_by_step,
  COUNT(DISTINCT user_pseudo_id) users_number,
FROM steps
GROUP BY funnel_step, rank_by_step
ORDER BY 2, 3 DESC),
-- TOP 3 countries by users number
TOP_3 AS(
SELECT
country,
COUNT(event_name) users
FROM date_format
GROUP BY country
ORDER BY 2 DESC
LIMIT 3),
--
event_rank_users_country AS(
SELECT
  funnel_step,
  COUNT(DISTINCT user_pseudo_id) users_count,
  ROUND(COUNT(DISTINCT user_pseudo_id)/(SELECT users FROM TOP_3 WHERE country = 'Canada')*100, 2) Perc,
FROM steps
WHERE country = 'Canada'
GROUP BY funnel_step
ORDER BY 2 DESC
),
top_countries_table AS(
SELECT
  funnel_step,
  SUM(CASE WHEN country = 'United States' THEN 1 ELSE 0 END) United_States,
  SUM(CASE WHEN country = 'India' THEN 1 ELSE 0 END) India,
  SUM(CASE WHEN country = 'Canada' THEN 1 ELSE 0 END) Canada,
  COUNT(user_pseudo_id) All_3
FROM steps
WHERE country IN (SELECT country FROM Top_3)
GROUP BY funnel_step
ORDER BY 2 DESC
)
SELECT
  *,
  ROUND(All_3/COALESCE(LAG(All_3, 1) OVER (ORDER BY All_3 DESC), All_3)*100,1) All_per_change,
  ROUND(United_States/COALESCE(LAG(United_States, 1) OVER (ORDER BY United_States DESC), United_States)*100,1) US_per_change,
  ROUND(India/COALESCE(LAG(India, 1) OVER (ORDER BY India DESC), India)*100,1) India_per_change,
  ROUND(Canada/COALESCE(LAG(Canada, 1) OVER (ORDER BY Canada DESC), Canada)*100,1) Canada_per_change,
FROM top_countries_table
ORDER BY All_3 DESC
/*
SELECT
  *,
  ROUND(((SELECT MAX(Perc) FROM event_rank_users_country) - Perc)/2, 2) Helper
FROM event_rank_users_country
GROUP BY funnel_step, users_count, Perc
ORDER BY 2 DESC
*/
