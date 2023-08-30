/*Update your 2.1 query by adding the name of the location and also add the average
 days amount between actual start date and actual end date per each location.*/
SELECT
  work_order.LocationID,
  location.Name Location_name,
  COUNT(DISTINCT work_order.WorkOrderID) no_work_orders,
  COUNT(DISTINCT work_order.ProductID) no_uniq_products,
  SUM(work_order.ActualCost) actual_cost,
  ROUND(AVG(DATE_DIFF(work_order.ActualEndDate, work_order.ActualStartDate, DAY)), 2) average_days
FROM
  `tc-da-1.adwentureworks_db.workorderrouting` AS work_order
JOIN
  `tc-da-1.adwentureworks_db.location` AS location
ON
  work_order.LocationID = location.LocationID
WHERE
  DATE_TRUNC(work_order.ActualStartDate, MONTH) = '2004-01-01'
GROUP BY
  work_order.LocationID,
  location.Name
ORDER BY actual_cost DESC
