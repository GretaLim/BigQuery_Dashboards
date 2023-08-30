-- Select all the expensive work Orders (above 300 actual cost) that happened throught January 2004.
SELECT
  WorkOrderID,
  SUM(ActualCost) actual_cost
FROM
  `tc-da-1.adwentureworks_db.workorderrouting`
WHERE
  DATE_TRUNC(ActualStartDate, MONTH) = '2004-01-01'
GROUP BY
  WorkOrderID
HAVING
  actual_cost > 300
  --ORDER BY 2 DESC
