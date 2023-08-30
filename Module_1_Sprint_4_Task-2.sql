/*Reporting Sales’ numbers
Main tables to start from: salesorderheader.
2.1 Create a query of monthly sales numbers in each Country & region. Include in the query a number of orders, customers and sales persons in each month with a total amount with tax earned. Sales numbers from all types of customers are required.*/
--
WITH sales_order_info AS (
SELECT
  --LEFT(CAST(DATE_TRUNC(sales.OrderDate, YEAR) AS STRING), 4) year,
  LAST_DAY(CAST(DATE_TRUNC(sales.OrderDate, MONTH) AS DATE), MONTH) Period,
  sales.TerritoryID regionID,
  COUNT( sales.SalesOrderID) orders_no,
  COUNT (DISTINCT sales.CustomerID) customer_no,
  COUNT (DISTINCT sales.SalesPersonID) sales_person_no,
  ROUND(SUM(sales.TotalDue), 0) sales_w_tax
FROM
  `adwentureworks_db.salesorderheader` sales
GROUP BY regionID, Period
),
--
country_region_info AS(
SELECT
  territory.TerritoryID regionID,
  territory.Name Region,
  country.Name Country
FROM `adwentureworks_db.salesterritory` as territory
JOIN `adwentureworks_db.countryregion` country
ON territory.CountryRegionCode = country.CountryRegionCode
),
--
task_2_1 AS(
  SELECT
    sales_ord.Period,
    region.Country,
    region.Region,
    sales_ord.orders_no,
    sales_ord.customer_no,
    sales_ord.sales_person_no,
    sales_ord.sales_w_tax,
    region.regionID
  FROM sales_order_info AS sales_ord
  JOIN country_region_info AS region
  ON sales_ord.regionID = region.regionID
  ORDER BY Country DESC, regionID, DATE_TRUNC(sales_ord.Period, YEAR) DESC, sales_ord.Period
),

--
/*2.2 Enrich 2.1 query with the cumulative_sum of the total amount with tax earned per country & region.*/
--
task_2_2 AS (
  SELECT
    *,
    SUM(Sales_w_tax) OVER(PARTITION BY task_2_1.regionID ORDER BY task_2_1.Period) cumulative_sum
  FROM task_2_1
  ORDER BY region
),
--
/*2.3 Enrich 2.2 query by adding ‘sales_rank’ column that ranks rows from best to worst for each country based on total amount with tax earned each month. I.e. the month where the (US, Southwest) region made the highest total amount with tax earned will be ranked 1 for that region and vice versa.*/
--
task_2_3 AS (
  SELECT
    task_2_2.Period,
    task_2_2.Country,
    task_2_2.Region,
    task_2_2.orders_no,
    task_2_2.customer_no,
    task_2_2.sales_person_no,
    task_2_2.sales_w_tax,
    ROW_NUMBER() OVER(PARTITION BY task_2_2.regionID ORDER BY task_2_2.sales_w_tax DESC) region_sales_rank,
    --ROW_NUMBER() OVER(PARTITION BY task_2_2.Country ORDER BY task_2_2.sales_w_tax DESC) country_sales_rank,
    task_2_2.cumulative_sum,
    task_2_2.regionID
  FROM task_2_2
),
--
/*2.4 Enrich 2.3 query by adding taxes on a country level:
As taxes can vary in country based on province, the needed column is ‘mean_tax_rate’ -> average tax rate in a country.
Also, as not all regions have data on taxes, you also want to be transparent and show the ‘perc_provinces_w_tax’ -> a column representing the percentage of provinces with available tax rates for each country (i.e. If US has 53 provinces, and 10 of them have tax rates, then for US it should show 0,19)*/
--
--tax rate by territory
tax_rates_region AS(
  SELECT
    territory.CountryRegionCode,
    territory.TerritoryID,
    territory.Name Territory_name,
    state.stateProvinceID,
    state.Name State_name,
    tax_rate.TaxRate,
    tax_rate.name Tax_name,
    --SUM(tax_rate.TaxRate) OVER (PARTITION BY territory.CountryRegionCode),
    ROUND(AVG(tax_rate.TaxRate) OVER (PARTITION BY territory.CountryRegionCode), 2) mean_tax_rate,
    round(COUNT(tax_rate.TaxRate) OVER (PARTITION BY territory.CountryRegionCode) / COUNT(*) OVER (PARTITION BY territory.CountryRegionCode), 2) perc_provinces_w_tax
  FROM `adwentureworks_db.salesterritory` AS territory
  LEFT JOIN `adwentureworks_db.stateprovince` AS state
  ON territory.TerritoryID = state.TerritoryID
  LEFT JOIN `adwentureworks_db.salestaxrate` tax_rate
  ON state.StateProvinceID = tax_rate.StateProvinceID),
--
task_2_4 AS(
  SELECT
    task_2_3.*,
    tax_table.mean_tax_rate,
    tax_table.perc_provinces_w_tax
  FROM task_2_3
  JOIN (SELECT
          DISTINCT TerritoryID,
          CountryRegionCode,
          mean_tax_rate,
          perc_provinces_w_tax
        FROM tax_rates_region
  ) tax_table
  ON task_2_3.regionID = tax_table.TerritoryID
  ORDER BY task_2_3.Country DESC, task_2_3.Region DESC)
--
--Depenting on which task results should be shown, in the statement FROM must contain task_2_1, task_2_2, task_2_3 or task_2_4 CTE name
SELECT
  * EXCEPT(regionID)
FROM task_2_4
--WHERE task_2_3.Region = 'France'
