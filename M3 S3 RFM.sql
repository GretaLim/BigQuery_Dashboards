-- calculated additional columns and added filter to included just 12 month invoice data and exclude invoices without customer ID
WITH data_12m_period AS(
SELECT
  DATE_DIFF(DATE '2011-12-01', DATE(FORMAT_DATE('%Y-%m-%d',InvoiceDate)), DAY) ActiveDays,
  DATE_DIFF(DATE '2011-12-01', DATE(FORMAT_DATE('%Y-%m-%d',InvoiceDate)), MONTH) ActiveMonth,
  *,
  Quantity*UnitPrice TotalPrice
FROM
  `tc-da-1.turing_data_analytics.rfm`
WHERE
  DATE_TRUNC(InvoiceDate, DAY) BETWEEN '2010-12-01' AND '2011-12-01' AND
  CustomerID IS NOT NULL
  AND Quantity > 0
),
-- aggregated data by customer and invoice (excluded products)
Customer_invoice_details AS(
SELECT
  data_12m_period.ActiveDays,
  ActiveMonth,
  InvoiceNo,
  CustomerID,
  Country,
  SUM(Quantity) ItemsQuantity,
  ROUND(SUM(data_12m_period.TotalPrice), 2) InvoicePrice
  --COUNT(*)
FROM data_12m_period
-- There are 40 records with UnitPrice 0.0
GROUP BY InvoiceNo, CustomerID, Country, Activedays, ActiveMONTH),
-- calculate metrics for RFM analysis, aggregated by customer
agrregated_by_customer AS(
SELECT
  CustomerID,
  COUNT(InvoiceNo) Frequency,
  SUM(ItemsQuantity) TotalItems,
  ROUND(AVG(ItemsQuantity), 2) AverageItemsInv,
  ROUND(SUM(InvoicePrice), 2) TotalSales,
  ROUND(AVG(InvoicePrice), 2) AOV,
  MIN(ActiveDays) Recency_days,
  MIN(ActiveMONTH) LastInvoiceMonth,
  MAX(ActiveMONTH) OldestInvoiceMonth
FROM Customer_invoice_details
GROUP BY CustomerID
HAVING TotalSales > 0
),
/* to test with the result in TC
SELECT
  CustomerID,
  Recency_days,
  Frequency,
  TotalSales
FROM agrregated_by_customer
--WHERE agrregated_by_customer.Frequency <= 35 AND TotalSales between 30 and 8001
--      AND agrregated_by_customer.LastInvoiceMonth < 11 AND agrregated_by_customer.OldestInvoiceMonth > 1
ORDER BY agrregated_by_customer.customerID*/
Quartiles AS (
SELECT
    agrregated_by_customer.*,
    --All percentiles for MONETARY
    monetary.percentiles[offset(25)] AS m25,
    monetary.percentiles[offset(50)] AS m50,
    monetary.percentiles[offset(75)] AS m75,
    monetary.percentiles[offset(100)] AS m100,
    --All percentiles for FREQUENCY
    freq.percentiles[offset(25)] AS f25,
    freq.percentiles[offset(50)] AS f50,
    freq.percentiles[offset(75)] AS f75,
    freq.percentiles[offset(100)] AS f100,
    --All percentiles for RECENCY
    recency.percentiles[offset(25)] AS r25,
    recency.percentiles[offset(50)] AS r50,
    recency.percentiles[offset(75)] AS r75,
    recency.percentiles[offset(100)] AS r100
FROM
    agrregated_by_customer,
    (SELECT APPROX_QUANTILES(TotalSales, 100) percentiles FROM agrregated_by_customer) monetary,
    (SELECT APPROX_QUANTILES(Frequency, 100) percentiles FROM agrregated_by_customer) freq,
    (SELECT APPROX_QUANTILES(Recency_days, 100) percentiles FROM agrregated_by_customer) recency
),
Score AS (
    SELECT *,
    CAST(ROUND((f_score + m_score) / 2, 0) AS INT64) AS fm_score
    FROM (
        SELECT *,
        CASE WHEN TotalSales <= m25 THEN 1
            WHEN TotalSales <= m50 AND TotalSales > m25 THEN 2
            WHEN TotalSales <= m75 AND TotalSales > m50 THEN 3
            WHEN TotalSales <= m100 AND TotalSales > m75 THEN 4
        END AS m_score,
        CASE WHEN Frequency <= f25 THEN 1
            WHEN Frequency <= f50 AND Frequency > f25 THEN 2
            WHEN Frequency <= f75 AND Frequency > f50 THEN 3
            WHEN Frequency <= f100 AND Frequency > f75 THEN 4
        END AS f_score,
        --Recency scoring is reversed
        CASE WHEN Recency_days <= r25 THEN 4
            WHEN Recency_days <= r50 AND Recency_days > r25 THEN 3
            WHEN Recency_days <= r75 AND Recency_days > r50 THEN 2
            WHEN Recency_days <= r100 AND Recency_days> r75 THEN 1
        END AS r_score,
        FROM Quartiles
        )
)
SELECT
  *
FROM Score
--LIMIT 1
