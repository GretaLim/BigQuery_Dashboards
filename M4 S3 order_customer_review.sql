WITH review_score AS(
SELECT
  order_id,
  COUNT(DISTINCT review_id) review_count,
  ROUND(AVG (review_score),1) avg_score,
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
GROUP BY 1)
SELECT
  ordert.* ,
  customer.* EXCEPT(customer_id),
  review_score.* EXCEPT(order_id)
FROM `tc-da-1.olist_db.olist_orders_dataset` ordert
LEFT JOIN `tc-da-1.olist_db.olist_customesr_dataset` customer
ON customer.customer_id = ordert.customer_id
LEFT JOIN review_score
ON ordert.order_id = review_score.order_id
LIMIT 1000
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
