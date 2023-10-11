/* This part to check, which users made subscriptions few times and if the final end date correct
few_times_user AS(
  SELECT
    user_pseudo_id,
  COUNT(subscription_start) counts,
  --MAX(subscription_end)
  --DISTINCT (EXTRACT(WEEK(MONDAY) FROM subscription_start)) week--,
  --EXTRACT (YEAR FROM subscription_start) year,
FROM
  subscriptions_end_date
GROUP BY user_pseudo_id
HAVING COUNT(subscription_start)  > 1
),*/
--
/*
Total obervations 274 362
Users 270 154
3 categories: desktop, mobile, tablet
50 different countries
Start date range: 2020-11-01 - 2021-01-31 (15 weeks: since the Sunday until the Sunday)
28 837 of observations has an end date and the range is: 2020-11-02 - 2021-01-31

TASK:

You should provide weekly subscriptions data that shows how many subscribers started their subscription in a particular week and how many remain active in the following 6 weeks. Your end result should show weekly retention cohorts for each week of data available in the dataset and their retention from week 0 to week 6. Assume that you are doing this analysis on 2021-02-07.
*/
-- I specified what could be MAX date, as there are subscription_end with null values
WITH today AS(
  SELECT
    CAST('2021-02-07' AS DATE) AS max_date
    --CAST(CURRENT_DATE() AS DATE) AS max_date
  ),
-- changed subscription's end date to the 
MIN_MAX_range AS(
SELECT
  user_pseudo_id,
  MIN(DATE_TRUNC(subscription_start, week(MONDAY))) start_week,
  MAX(DATE_TRUNC(COALESCE(subscription_end, (SELECT max_date FROM today)), week(MONDAY))) end_week
FROM
  `turing_data_analytics.subscriptions`
GROUP BY user_pseudo_id),
--
Retention AS(
SELECT
  MIN_MAX_range.start_week,
  CONCAT (MIN_MAX_range.start_week, ' - ', DATE_ADD(MIN_MAX_range.start_week, interval 6 DAY)) First_subcription_week,
  COUNT(user_pseudo_id) Cohort_size,
  --SUM(CASE WHEN MIN_MAX_range.start_week IS NOT NULL THEN 1 ELSE 0 END) Cohort_size_1
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 1 WEEK) THEN 1 ELSE NULL END) WEEK_0,
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 2 WEEK) THEN 1 ELSE NULL END) WEEK_1,
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 3 WEEK) THEN 1 ELSE NULL END) WEEK_2,
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 4 WEEK) THEN 1 ELSE NULL END) WEEK_3,
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 5 WEEK) THEN 1 ELSE NULL END) WEEK_4,
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 6 WEEK) THEN 1 ELSE NULL END) WEEK_5,
  SUM(CASE WHEN MIN_MAX_range.end_week >= DATE_ADD(MIN_MAX_range.start_week, interval 7 WEEK) THEN 1 ELSE NULL END) WEEK_6,
FROM MIN_MAX_range
GROUP BY MIN_MAX_range.start_week
ORDER BY 1
)
SELECT
  Retention.start_week,
  Retention.First_subcription_week,
  Retention.Cohort_size,
  ROUND(Retention.WEEK_0/Retention.Cohort_size, 5) WEEK_0,
  ROUND(Retention.WEEK_1/Retention.Cohort_size, 5) WEEK_1,
  ROUND(Retention.WEEK_2/Retention.Cohort_size, 5) WEEK_2,
  ROUND(Retention.WEEK_3/Retention.Cohort_size, 5) WEEK_3,
  ROUND(Retention.WEEK_4/Retention.Cohort_size, 5) WEEK_4,
  ROUND(Retention.WEEK_5/Retention.Cohort_size, 5) WEEK_5,
  ROUND(Retention.WEEK_6/Retention.Cohort_size, 5) WEEK_6
FROM Retention
