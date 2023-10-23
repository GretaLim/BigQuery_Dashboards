-- There are in total 270 154 users
-- Period from 2020-11-01 until 2021-01-31
-- Every customer is making purchases just in one country
-- Events with value zero I will not include in calculations as purchased
WITH
period_end AS(
  SELECT
    CAST('2021-01-31' AS DATE) AS last_week
  ),
analysis_data AS(
SELECT
  PARSE_DATE('%Y%m%d',event_date) event_date_f,
  event_timestamp,
  event_name,
  event_value_in_usd, --362165.0
  user_pseudo_id,
  country,
  IF(event_name = 'purchase' AND event_value_in_usd IS NOT NULL, DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), week(SUNDAY)), NULL) purchase_week,
  MIN(DATE_TRUNC(PARSE_DATE('%Y%m%d',event_date), week(SUNDAY))) OVER (PARTITION BY user_pseudo_id) first_visit_week
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE PARSE_DATE('%Y%m%d',event_date) < (SELECT last_week FROM period_end)
),
Cohort_avg_revenue AS(
SELECT
  ad.first_visit_week,
  CONCAT (ad.first_visit_week, ' - ', DATE_ADD(ad.first_visit_week, interval 6 DAY)) First_week,
  COUNT (DISTINCT ad.user_pseudo_id) New_users,
  SUM(ad.event_value_in_usd) revenue_by_user_week,
  ROUND(SUM(ad.event_value_in_usd)/COUNT (DISTINCT ad.user_pseudo_id), 4) avg_revenue_cust,
  ROUND(SUM( CASE WHEN purchase_week = first_visit_week THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id), 4) WEEK_0,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 1 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_1,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 2 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_2,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 3 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_3,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 4 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_4,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 5 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_5,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 6 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_6,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 7 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_7,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 8 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_8,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 9 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_9,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 10 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_10,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 11 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_11,
  ROUND(SUM( CASE WHEN purchase_week = DATE_ADD(first_visit_week, interval 12 WEEK) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_12
FROM analysis_data ad
GROUP BY ad.first_visit_week ),
Cumulative_average_revenue AS(
SELECT
  ad.first_visit_week,
  CONCAT (ad.first_visit_week, ' - ', DATE_ADD(ad.first_visit_week, interval 6 DAY)) First_week,
  COUNT (DISTINCT ad.user_pseudo_id) New_users,
  ROUND(SUM( CASE WHEN purchase_week = first_visit_week THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id), 4) WEEK_0,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 1 WEEK) AND DATE_ADD(first_visit_week, interval 1 WEEK) < (SELECT last_week FROM period_end) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_1,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 2 WEEK) AND DATE_ADD(first_visit_week, interval 2 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_2,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 3 WEEK) AND DATE_ADD(first_visit_week, interval 3 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_3,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 4 WEEK) AND DATE_ADD(first_visit_week, interval 4 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_4,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 5 WEEK) AND DATE_ADD(first_visit_week, interval 5 WEEK) < (SELECT last_week FROM period_end) THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_5,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 6 WEEK) AND DATE_ADD(first_visit_week, interval 6 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_6,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 7 WEEK) AND DATE_ADD(first_visit_week, interval 7 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_7,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 8 WEEK) AND DATE_ADD(first_visit_week, interval 8 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_8,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 9 WEEK) AND DATE_ADD(first_visit_week, interval 9 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_9,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 10 WEEK) AND DATE_ADD(first_visit_week, interval 10 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_10,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 11 WEEK) AND DATE_ADD(first_visit_week, interval 11 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_11,
  ROUND(SUM( CASE WHEN purchase_week <= DATE_ADD(first_visit_week, interval 12 WEEK) AND DATE_ADD(first_visit_week, interval 12 WEEK) < (SELECT last_week FROM period_end)THEN event_value_in_usd ELSE NULL END)/COUNT (DISTINCT ad.user_pseudo_id),4) WEEK_12
FROM analysis_data ad
GROUP BY ad.first_visit_week),
Cumulative_average AS(
  SELECT
    'Cumulative average' Metric,
    ROUND(AVG(WEEK_0), 4) WEEK_0,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1), 4) WEEK_1,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2), 4) WEEK_2,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3), 4) WEEK_3,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4), 4) WEEK_4,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5), 4) WEEK_5,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6), 4) WEEK_6,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6)+AVG(WEEK_7), 4) WEEK_7,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6)+AVG(WEEK_7)+AVG(WEEK_8), 4) WEEK_8,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6)+AVG(WEEK_7)+AVG(WEEK_8)+AVG(WEEK_9), 4) WEEK_9,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6)+AVG(WEEK_7)+AVG(WEEK_8)+AVG(WEEK_9)+AVG(WEEK_10), 4) WEEK_10,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6)+AVG(WEEK_7)+AVG(WEEK_8)+AVG(WEEK_9)+AVG(WEEK_10)+AVG(WEEK_11), 4) WEEK_11,
    ROUND(AVG(WEEK_0)+AVG(WEEK_1)+AVG(WEEK_2)+AVG(WEEK_3)+AVG(WEEK_4)+AVG(WEEK_5)+AVG(WEEK_6)+AVG(WEEK_7)+AVG(WEEK_8)+AVG(WEEK_9)+AVG(WEEK_10)+AVG(WEEK_11)+AVG(WEEK_12), 4) WEEK_12
  FROM Cohort_avg_revenue
),
Cumulative_growth AS(
  SELECT
    'Cumulative growth, %' Metric,
    NULL WEEK_0,
    ROUND((WEEK_1/WEEK_0-1)*100 , 2) WEEK_1,
    ROUND((WEEK_2/WEEK_1-1)*100 , 2) WEEK_2,
    ROUND((WEEK_3/WEEK_2-1)*100 , 2) WEEK_3,
    ROUND((WEEK_4/WEEK_3-1)*100 , 2) WEEK_4,
    ROUND((WEEK_5/WEEK_4-1)*100 , 2) WEEK_5,
    ROUND((WEEK_6/WEEK_5-1)*100 , 2) WEEK_6,
    ROUND((WEEK_7/WEEK_6-1)*100 , 2) WEEK_7,
    ROUND((WEEK_8/WEEK_7-1)*100 , 2) WEEK_8,
    ROUND((WEEK_9/WEEK_8-1)*100 , 2) WEEK_9,
    ROUND((WEEK_10/WEEK_9-1)*100 , 2) WEEK_10,
    ROUND((WEEK_11/WEEK_10-1)*100 , 2) WEEK_11,
    ROUND((WEEK_12/WEEK_11-1)*100 , 2) WEEK_12,
  FROM Cumulative_average
),
Metrics_table AS(
SELECT
  *
FROM Cumulative_average
UNION ALL
SELECT
  *
FROM Cumulative_growth),
base_amounts AS(
SELECT
  first_visit_week,
  Cumulative_average_revenue.New_users,
  COALESCE(IF (WEEK_1 IS NULL, WEEK_0, NULL), 0)+COALESCE(IF (WEEK_2 IS NULL, WEEK_1, NULL), 0)+COALESCE(IF (WEEK_3 IS NULL, WEEK_2, NULL), 0)
  +COALESCE(IF (WEEK_4 IS NULL, WEEK_3, NULL), 0)+COALESCE(IF (WEEK_5 IS NULL, WEEK_4, NULL), 0)+COALESCE(IF (WEEK_5 IS NULL, WEEK_4, NULL), 0)+COALESCE(IF (WEEK_5 IS NULL, WEEK_4, NULL), 0)+COALESCE(IF (WEEK_6 IS NULL, WEEK_5, NULL), 0)+COALESCE(IF (WEEK_7 IS NULL, WEEK_6, NULL), 0)+COALESCE(IF (WEEK_8 IS NULL, WEEK_7, NULL), 0)+COALESCE(IF (WEEK_9 IS NULL, WEEK_8, NULL), 0)+COALESCE(IF (WEEK_10 IS NULL, WEEK_9, NULL), 0)+COALESCE(IF (WEEK_11 IS NULL, WEEK_10, NULL), 0)+COALESCE(IF (WEEK_12 IS NULL, WEEK_11, NULL), 0) amount_base
FROM Cumulative_average_revenue),

Multiplication AS (
SELECT
  *,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 1 THEN NULL ELSE (SELECT ROUND(1+WEEK_1/100,4) FROM Cumulative_growth) END WEEK_1,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 2 THEN NULL ELSE (SELECT ROUND(1+WEEK_2/100,4) FROM Cumulative_growth) END WEEK_2,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 3 THEN NULL ELSE (SELECT ROUND(1+WEEK_3/100,4) FROM Cumulative_growth) END WEEK_3,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 4 THEN NULL ELSE (SELECT ROUND(1+WEEK_4/100,4) FROM Cumulative_growth) END WEEK_4,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 5 THEN NULL ELSE (SELECT ROUND(1+WEEK_5/100,4) FROM Cumulative_growth) END WEEK_5,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 6 THEN NULL ELSE (SELECT ROUND(1+WEEK_6/100,4) FROM Cumulative_growth) END WEEK_6,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 7 THEN NULL ELSE (SELECT ROUND(1+WEEK_7/100,4) FROM Cumulative_growth) END WEEK_7,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 8 THEN NULL ELSE (SELECT ROUND(1+WEEK_8/100,4) FROM Cumulative_growth) END WEEK_8,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 9 THEN NULL ELSE (SELECT ROUND(1+WEEK_9/100,4) FROM Cumulative_growth) END WEEK_9,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 10 THEN NULL ELSE (SELECT ROUND(1+WEEK_10/100,4) FROM Cumulative_growth) END WEEK_10,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 11 THEN NULL ELSE (SELECT ROUND(1+WEEK_11/100,4) FROM Cumulative_growth) END WEEK_11,
  CASE WHEN DATE_DIFF((SELECT last_week FROM period_end), first_visit_week, WEEK) > 12 THEN NULL ELSE (SELECT ROUND(1+WEEK_12/100,4) FROM Cumulative_growth) END WEEK_12
FROM base_amounts)
SELECT
  first_visit_week,
  ROUND(WEEK_1 *amount_base, 4) WEEK_1_p,
  ROUND(IFNULL(WEEK_1, 1)*WEEK_2*amount_base, 4) WEEK_2_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*WEEK_3*amount_base, 4) WEEK_3_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*WEEK_4*amount_base, 4) WEEK_4_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*WEEK_5*amount_base, 4) WEEK_5_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*WEEK_6*amount_base, 4) WEEK_6_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*WEEK_7*amount_base, 4) WEEK_7_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*IFNULL(WEEK_7, 1)*WEEK_8*amount_base, 4) WEEK_8_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*IFNULL(WEEK_7, 1)*IFNULL(WEEK_8, 1)*WEEK_9*amount_base, 4) WEEK_9_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*IFNULL(WEEK_7, 1)*IFNULL(WEEK_8, 1)*IFNULL(WEEK_9, 1)*WEEK_10*amount_base, 4) WEEK_10_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*IFNULL(WEEK_7, 1)*IFNULL(WEEK_8, 1)*IFNULL(WEEK_9, 1)*IFNULL(WEEK_10, 1)*WEEK_11*amount_base, 4) WEEK_11_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*IFNULL(WEEK_7, 1)*IFNULL(WEEK_8, 1)*IFNULL(WEEK_9, 1)*IFNULL(WEEK_10, 1)*IFNULL(WEEK_11, 1)*WEEK_12*amount_base, 4) WEEK_12_p,
  ROUND(IFNULL(WEEK_1, 1)*IFNULL(WEEK_2, 1)*IFNULL(WEEK_3, 1)*IFNULL(WEEK_4, 1)*IFNULL(WEEK_5, 1)*IFNULL(WEEK_6, 1)*IFNULL(WEEK_7, 1)*IFNULL(WEEK_8, 1)*IFNULL(WEEK_9, 1)*IFNULL(WEEK_10, 1)*IFNULL(WEEK_11, 1)*WEEK_12*amount_base*New_users/(SELECT SUM(New_users) FROM Cohort_avg_revenue), 4) WAVG_WEEK_12_p
FROM Multiplication
ORDER BY first_visit_week
