WITH today AS(
  SELECT
    CAST('2021-02-07' AS DATE) AS max_date
  ),
MIN_MAX_range AS(
SELECT
  user_pseudo_id,
  MIN(DATE_TRUNC(subscription_start, week(MONDAY))) start_week,
  MAX(DATE_TRUNC(COALESCE(subscription_end, (SELECT max_date FROM today)), week(MONDAY))) end_week
FROM
  `turing_data_analytics.subscriptions`
GROUP BY user_pseudo_id)
--
SELECT
  MIN_MAX_range.start_week,
  CONCAT (MIN_MAX_range.start_week, ' - ', DATE_ADD(MIN_MAX_range.start_week, interval 6 DAY)) First_subcription_week,
  COUNT(user_pseudo_id) Cohort_size,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-10-26' AND MIN_MAX_range.end_week >= '2020-10-26' THEN 1 ELSE NULL END) y20_10_26,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-11-02' AND MIN_MAX_range.end_week >= '2020-11-02'THEN 1 ELSE NULL END) y20_11_02,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-11-09' AND MIN_MAX_range.end_week >= '2020-11-09' THEN 1 ELSE NULL END) y20_11_09,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-11-16' AND MIN_MAX_range.end_week >= '2020-11-16' THEN 1 ELSE NULL END) y20_11_16,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-11-23' AND MIN_MAX_range.end_week >= '2020-11-23' THEN 1 ELSE NULL END) y20_11_23,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-11-30' AND MIN_MAX_range.end_week >= '2020-11-30' THEN 1 ELSE NULL END) y20_11_30,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-12-07' AND MIN_MAX_range.end_week >= '2020-12-07' THEN 1 ELSE NULL END) y20_12_07,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-12-14' AND MIN_MAX_range.end_week >= '2020-12-14' THEN 1 ELSE NULL END) y20_12_14,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-12-21' AND MIN_MAX_range.end_week >= '2020-12-21' THEN 1 ELSE NULL END) y20_12_21,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2020-12-28' AND MIN_MAX_range.end_week >= '2020-12-28' THEN 1 ELSE NULL END) y20_12_28,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2021-01-04' AND MIN_MAX_range.end_week >= '2021-01-04'  THEN 1 ELSE NULL END) y21_01_04,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2021-01-11' AND MIN_MAX_range.end_week >= '2021-01-11' THEN 1 ELSE NULL END) y21_01_11,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2021-01-18' AND MIN_MAX_range.end_week >= '2021-01-18' THEN 1 ELSE NULL END) y21_01_18,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2021-01-25' AND MIN_MAX_range.end_week >= '2021-01-25' THEN 1 ELSE NULL END) y21_01_25,
  SUM(CASE WHEN MIN_MAX_range.start_week <= '2021-02-01' AND MIN_MAX_range.end_week >= '2021-02-01' THEN 1 ELSE NULL END) y21_02_01,
FROM MIN_MAX_range
GROUP BY MIN_MAX_range.start_week 
