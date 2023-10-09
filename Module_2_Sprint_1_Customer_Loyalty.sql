WITH customer_year AS(
SELECT
  *,
  LEFT(CAST(DATE_TRUNC(OrderDate, Year) AS STRING), 4) AS YEAR
FROM `tc-da-1.adwentureworks_db.salesorderheader`
WHERE SalesPersonID IS NOT NULL
)
--
SELECT
  DISTINCT CustomerID customer,
  Count(*) orders_count,
  round(AVG(TotalDue), 2) AverageOrderAmount,
  SUM(TotalDue) TotalSales,
  MIN(customer_year.YEAR) MinYear,
  MAX(customer_year.YEAR) MaxYear,
  CASE
    WHEN MIN(customer_year.YEAR) = '2001' AND MAX(customer_year.YEAR) <> '2001' THEN '2001 customer'
    WHEN MIN(customer_year.YEAR) = '2002' AND MAX(customer_year.YEAR) <> '2002' THEN '2002 customer'
    WHEN MIN(customer_year.YEAR) = '2003' AND MAX(customer_year.YEAR) <> '2003' THEN '2003 customer'
    WHEN MIN(customer_year.YEAR) = '2004' THEN '2004 customer'
    WHEN (MIN(customer_year.YEAR) = '2001' AND MAX(customer_year.YEAR) = '2001')
          OR (MIN(customer_year.YEAR) = '2002' AND MAX(customer_year.YEAR) = '2002')
          OR (MIN(customer_year.YEAR) = '2003' AND MAX(customer_year.YEAR) = '2003') THEN 'Lost'
  ELSE 'Other'
  END AS Loyal_customer
FROM customer_year
GROUP BY customer--, YEAR
ORDER BY 2 DESC
