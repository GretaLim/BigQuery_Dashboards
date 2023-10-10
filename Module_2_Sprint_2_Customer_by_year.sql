WITH customer_year_sales AS(
SELECT
  DISTINCT CustomerID,
  LEFT(CAST(DATE_TRUNC(OrderDate, Year) AS STRING), 4) AS YEAR,
  ROUND(SUM(TotalDue),2) Sales_by_customer
FROM `tc-da-1.adwentureworks_db.salesorderheader`
WHERE SalesPersonID IS NOT NULL
--GROUP BY ROLLUP (CustomerID, year)
GROUP BY CustomerID, year
ORDER BY 1, 2 )
SELECT
  CustomerID,
  CASE
    WHEN MIN(YEAR) = '2001' AND MAX(YEAR) <> '2001' THEN '2001 customer'
    WHEN MIN(YEAR) = '2002' AND MAX(YEAR) <> '2002' THEN '2002 customer'
    WHEN MIN(YEAR) = '2003' AND MAX(YEAR) <> '2003' THEN '2003 customer'
    WHEN MIN(YEAR) = '2004' THEN '2004 customer'
    WHEN (MIN(YEAR) = '2001' AND MAX(YEAR) = '2001')
          OR (MIN(YEAR) = '2002' AND MAX(YEAR) = '2002')
          OR (MIN(YEAR) = '2003' AND MAX(YEAR) = '2003') THEN 'Lost'
  ELSE 'Other'
  END AS Customer_first_year,
  ROUND(SUM(Sales_by_customer), 2) Total_sales
FROM customer_year_sales
GROUP BY ROLLUP (CustomerID)
