WITH reasons_count AS(
  SELECT
    DISTINCT SalesOrderID OrderID,
    COUNT (SalesReasonID) reasons_no
  FROM
    `adwentureworks_db.salesorderheadersalesreason`
  GROUP BY SalesOrderID
)
--
SELECT
  salesheader.SalesOrderID,
  salesheader.OrderDate,
  LEFT(CAST(DATE_TRUNC(salesheader.OrderDate, Year) AS STRING), 4) AS YEAR,
  salesheader.CustomerID,
  salesheader.SalesPersonID,
  salesheader.TerritoryID,
  salesheader.TotalDue,
  CASE
    WHEN salesheader.SalesPersonID IS NULL THEN salesreason.Name
    ELSE 'Sales representative'
  END SalesReason,
  COALESCE(reasons_count.reasons_no, 1) CountReason,
  ROUND( salesheader.TotalDue / COALESCE(reasons_count.reasons_no, 1), 2) AdaptedTotalSales
FROM `tc-da-1.adwentureworks_db.salesorderheader` salesheader
LEFT JOIN `adwentureworks_db.salesorderheadersalesreason` salesreasonjoin
ON salesheader.SalesOrderID = salesreasonjoin.SalesOrderID
LEFT JOIN `adwentureworks_db.salesreason` salesreason
ON salesreason.SalesReasonID = salesreasonjoin.SalesReasonID
LEFT JOIN reasons_count
ON reasons_count.OrderID = salesheader.SalesOrderID
ORDER BY SalesOrderID DESC
LIMIT 1000
