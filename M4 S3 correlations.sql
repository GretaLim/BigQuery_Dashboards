WITH review_score AS(
SELECT
  order_id,
  COUNT(DISTINCT review_id) review_count,
  ROUND(AVG (review_score),1) avg_score,
  ROUND(sum (review_score),1) sum_score,
  MIN(review_creation_date) first_date_review,
  MAX(review_creation_date) last_date_review,
  MIN(review_answer_timestamp) first_date_answer,
  MAX(review_answer_timestamp) last_date_answer,
  ROUND(AVG(TIMESTAMP_DIFF(review_answer_timestamp, review_creation_date, DAY)),0) avg_answer_time_days,
  COUNTIF(review_comment_title_length = 0) no_comment_title,
  ROUND(AVG(CASE WHEN review_comment_title_length != 0 THEN review_comment_title_length ELSE NULL END),1) avg_comment_title,
  COUNTIF(review_comment_message_length = 0) no_comment_message,
  ROUND(AVG(CASE WHEN review_comment_message_length != 0 THEN review_comment_message_length ELSE NULL END),1) avg_comment_message,
FROM
  `tc-da-1.olist_db.olist_order_reviews_dataset`
GROUP BY 1),
all_tables AS(
SELECT
  ordert.* ,
  customer.* EXCEPT(customer_id),
  review_score.* EXCEPT(order_id)
FROM `tc-da-1.olist_db.olist_orders_dataset` ordert
LEFT JOIN `tc-da-1.olist_db.olist_customesr_dataset` customer
ON customer.customer_id = ordert.customer_id
LEFT JOIN review_score
ON ordert.order_id = review_score.order_id),
delivery_score AS(
SELECT
  DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp,DAY) delivery_days,
  DATE_DIFF(order_approved_at, order_purchase_timestamp,DAY) approved_days,
  DATE_DIFF(order_delivered_carrier_date, order_purchase_timestamp,DAY) carrier_days,
  DATE_DIFF(order_estimated_delivery_date, order_delivered_carrier_date,DAY) late_days,
  IF(order_estimated_delivery_date > order_delivered_customer_date, 0, 1) late_delivery,
  CASE WHEN avg_score >=1 AND avg_score < 2 THEN 1 ELSE 0 END review_score1,
  CASE WHEN avg_score >=2 AND avg_score < 3 THEN 1 ELSE 0 END review_score2,
  CASE WHEN avg_score >=3 AND avg_score < 4 THEN 1 ELSE 0 END review_score3,
  CASE WHEN avg_score >=4 AND avg_score < 5 THEN 1 ELSE 0 END review_score4,
  CASE WHEN avg_score >=5 THEN 1 ELSE 0 END review_score5,
  avg_score
  --*
  --DISTINCT(avg_score)--*
FROM all_tables)
SELECT
COrr(delivery_days,delivery_days),
COrr(delivery_days,approved_days),
COrr(delivery_days,carrier_days),
COrr(delivery_days,late_days),
COrr(delivery_days,review_score1),
COrr(delivery_days,review_score2),
COrr(delivery_days,review_score3),
COrr(delivery_days,review_score4),
COrr(delivery_days,review_score5),
COrr(delivery_days,avg_score),
COrr(delivery_days,late_delivery) late_delivery,
COrr(approved_days,delivery_days),
COrr(approved_days,approved_days),
COrr(approved_days,carrier_days),
COrr(approved_days,late_days),
COrr(approved_days,review_score1),
COrr(approved_days,review_score2),
COrr(approved_days,review_score3),
COrr(approved_days,review_score4),
COrr(approved_days,review_score5),
COrr(approved_days,avg_score),
COrr(approved_days,late_delivery) late_delivery1,
COrr(carrier_days,delivery_days),
COrr(carrier_days,approved_days),
COrr(carrier_days,carrier_days),
COrr(carrier_days,late_days),
COrr(carrier_days,review_score1),
COrr(carrier_days,review_score2),
COrr(carrier_days,review_score3),
COrr(carrier_days,review_score4),
COrr(carrier_days,review_score5),
COrr(carrier_days,avg_score),
COrr(carrier_days,late_delivery) late_delivery2,
COrr(late_days,delivery_days),
COrr(late_days,approved_days),
COrr(late_days,carrier_days),
COrr(late_days,late_days),
COrr(late_days,review_score1),
COrr(late_days,review_score2),
COrr(late_days,review_score3),
COrr(late_days,review_score4),
COrr(late_days,review_score5),
COrr(late_days,avg_score),
COrr(late_days,late_delivery) late_delivery3,
COrr(late_delivery,delivery_days),
COrr(late_delivery,approved_days),
COrr(late_delivery,carrier_days),
COrr(late_delivery,late_days),
COrr(late_delivery,review_score1),
COrr(late_delivery,review_score2),
COrr(late_delivery,review_score3),
COrr(late_delivery,review_score4),
COrr(late_delivery,review_score5),
COrr(late_delivery,avg_score),
COrr(late_delivery,late_delivery) late_delivery4,
FROM delivery_score
--WHERE order_id = '31854ea0f6e411311f9d52ad9f6b779a'
--LIMIT 1000
--
/*
SELECT
  COUNT(*) observations,
  COUNT(DISTINCT order_id) orders_count,
  COUNT(DISTINCT customer_id) customers_count,
  COUNT(DISTINCT order_status) order_status_count,
  MIN(order_purchase_timestamp) start_date,
  MAX(order_purchase_timestamp) end_date,
  COUNT(order_purchase_timestamp) if_all_orders_purchased,
  COUNT(order_approved_at) approved_orders,
  COUNT(order_delivered_carrier_date) delivered_orders,
  COUNT(order_estimated_delivery_date) orders_with_estimated_date
FROM
  `tc-da-1.olist_db.olist_orders_dataset` */
