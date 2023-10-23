SELECT
  order_id,
  payment_type,
  MAX(payment_sequential) lats_sequent,
  COUNT(payment_sequential) sequent_count,
  MAX(payment_installments) installment,
  ROUND(SUM(payment_value),2) total_payment_value,
  ROUND(AVG(payment_value),2) avg_payment_value
FROM `tc-da-1.olist_db.olist_order_payments_dataset`
--WHERE order_id IN('a079628ac8002126e75f86b0f87332e4')
GROUP BY 1, 2
