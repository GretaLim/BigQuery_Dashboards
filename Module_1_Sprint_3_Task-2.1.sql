/*Create an aggregated query to selec tthe:
Number of unique work orders.
Number of unique products.
Total actual cost.
For each location Id from the 'workoderrouting' table for orders in January 2004.*/
SELECT
  LocationID,
  COUNT(DISTINCT WorkOrderID) no_work_orders,
  COUNT(DISTINCT ProductID ) no_uniq_products,
  SUM(ActualCost) actual_cost
FROM
  `tc-da-1.adwentureworks_db.workorderrouting`
WHERE
  DATE_TRUNC(ActualStartDate, MONTH) = '2004-01-01'
GROUP BY
  LocationID
ORDER BY
  actual_cost DESC
