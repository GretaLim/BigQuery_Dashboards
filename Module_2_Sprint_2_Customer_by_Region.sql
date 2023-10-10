WITH territory_info AS(
  SELECT
  TerritoryID,
  CASE
    WHEN CountryRegionCode = 'US' THEN CONCAT(name, ', ',CountryRegionCode)
    ELSE name
  END AS region_name
  FROM `adwentureworks_db.salesterritory`
),
sales_order AS
(SELECT
  DISTINCT sales_order.CustomerID,
  sales_order.SalesPersonID,
  LEFT(CAST(DATE_TRUNC(sales_order.OrderDate, Year) AS STRING), 4) AS year,
  territory_info.TerritoryID,
  territory_info.region_name,
  MIN(sales_order.OrderDate) OVER (PARTITION BY region_name) MinOrderDate
  --SUM(sales_order.TotalDue) TotalSales
FROM `adwentureworks_db.salesorderheader` sales_order
LEFT JOIN territory_info
ON territory_info.TerritoryID = sales_order.TerritoryID
WHERE SalesPersonID IS NOT NULL
GROUP BY sales_order.CustomerID, sales_order.SalesPersonID, year,   territory_info.TerritoryID, territory_info.region_name, sales_order.OrderDate
),
by_territory_2004_year AS
(SELECT
  TerritoryID,
  region_name,
  COUNT (DISTINCT CustomerID) Active_2004,
  COUNT (DISTINCT salesPersonID) SR_2004
-- year
  FROM sales_order
WHERE sales_order.year = '2004'
GROUP BY region_name, TerritoryID
)

SELECT
  sales_order.TerritoryID,
  sales_order.region_name,
  COUNT(DISTINCT customer.CustomerID) customer_count,
  by_territory_2004_year.Active_2004,
  ROUND(by_territory_2004_year.Active_2004/COUNT(DISTINCT customer.CustomerID)*100,2) perc_active_2004,
  by_territory_2004_year.SR_2004,
  sales_order.MinOrderDate,
  CASE
	WHEN LEFT(CAST(DATE_TRUNC(sales_order.MinOrderDate, year) as STRING), 4) = '2001' THEN 'ST since 2001'
	WHEN LEFT(CAST(DATE_TRUNC(sales_order.MinOrderDate, year) as STRING), 4) = '2002' THEN 'ST since 2002'
	ELSE 'ST since 2003'
  END AS TerritoriesFirstYear
--  COUNT(DISTINCT customer.CustomerID) OVER(PARTITION BY sales_order.year) customer_count
FROM
  `tc-da-1.adwentureworks_db.customer` customer
LEFT JOIN sales_order
ON sales_order.CustomerID = customer.CustomerID
LEFT JOIN by_territory_2004_year
ON by_territory_2004_year.territoryID = sales_order.territoryID
WHERE SalesPersonID IS NOT NULL
GROUP BY sales_order.region_name, by_territory_2004_year.Active_2004, by_territory_2004_year.SR_2004, sales_order.MinOrderDate, sales_order.TerritoryID
ORDER BY perc_active_2004 DESC
