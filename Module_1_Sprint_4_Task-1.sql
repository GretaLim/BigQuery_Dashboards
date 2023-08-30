/* Write a query that provides:
Identity information : CustomerId, Firstname, Last Name, FullName (First Name & Last Name).
An Extra column called addressing_title i.e. (Mr. Achong), if the title is missing - Dear Achong.
Contact information : Email, phone, account number, CustomerType. */
  --
  -- needed info from contact table
  --
WITH
  contact_info AS (
  SELECT
    ContactId,
    FirstName,
    LastName,
    CONCAT(FirstName, ' ', LastName) FullName,
    CASE
      WHEN Title IS NULL THEN CONCAT('Dear ', LastName)
    ELSE
    CONCAT(Title, ' ', LastName)
  END
    addressing_title,
    EmailAddress,
    Phone
  FROM
    `adwentureworks_db.contact` ),
  --
  -- individual and customer table join
  --
  individual_info AS (
  SELECT
    individual.CustomerID,
    individual.ContactID contact_ID,
    customer.AccountNumber,
    customer.CustomerType
  FROM
    `adwentureworks_db.individual` AS individual
  LEFT JOIN
    `adwentureworks_db.customer` AS customer
  ON
    individual.CustomerID = customer.CustomerID ),
  --
  -- Location information : City, State & Country, address (Join of SalesTerritory table and column Territory Group needed for 4.1.4 task)
  --
  location_info AS (
  SELECT
    customer_address.CustomerID ID,
    address.City,
    address.AddressLine1,
    address.AddressLine2,
    state.name State,
    country.Name Country,
    territory.Group region
  FROM
    `adwentureworks_db.address` address
  LEFT JOIN
    `adwentureworks_db.stateprovince` AS state
  ON
    Address.StateProvinceID = state.StateProvinceID
  LEFT JOIN `adwentureworks_db.salesterritory` territory
  ON state.TerritoryID = territory.TerritoryID
  LEFT JOIN
    `adwentureworks_db.countryregion` AS country
  ON
    state.CountryRegionCode = country.CountryRegionCode
  RIGHT JOIN
    `adwentureworks_db.customeraddress` customer_address
  ON
    Address.AddressID = customer_address.AddressID ),
  --
  /*Sales: number of orders, total amount (with Tax), date of the last order.*/
  -- Salesorderhear table has 31465 observations - orders info
  -- Amount of customers 19119 in orders table
  -- Latest date of order 2004-07-31
  --
  sales_info AS(
  SELECT
    CustomerID,
    COUNT(SalesOrderID) order_amount,
    ROUND(SUM(TotalDue),2) Total_sales,
    MAX(OrderDate) Last_order_date
  FROM
    `tc-da-1.adwentureworks_db.salesorderheader`
  GROUP BY
    CustomerID),
  --
  /*1.1 You’ve been tasked to create a detailed overview of all individual customers (these are defined by customerType = ‘I’
  and/or stored in an individual table). */
  --
  -- JOIN of the temp. tables: individual_info, contact_info, location_info, sales_info
  --
  task_4_1_1 AS(
  SELECT
    individual_info.CustomerID,
    contact_info.* EXCEPT (ContactID),
    individual_info.AccountNumber,
    individual_info.CustomerType,
    location_info.* EXCEPT (ID, region),
    sales_info.* EXCEPT(CustomerID)
  FROM
    contact_info
  RIGHT JOIN
    individual_info
  ON
    contact_info.ContactID = individual_info.contact_ID
  LEFT JOIN
    location_info
  ON
    location_info.ID = individual_info.customerID
  LEFT JOIN
    sales_info
  ON
    individual_info.customerID = sales_info.customerID
  ORDER BY
    sales_info.total_sales DESC
    ),
--
/*1.2 Business finds the original query valuable to analyze customers and now want to get the data from the first query for the top 200 customers
with the highest total amount (with tax) who have not ordered for the last 365 days. How would you identify this segment?
Hints:
You can use temp table, cte and/or subquery of the 1.1 select.
Note that the database is old and the current date should be defined by finding the latest order date in the orders table. */
--
task_4_1_2 AS(
SELECT
  *
--, DATE_DIFF((SELECT MAX(Last_order_date) FROM sales_info), Last_order_date, DAY)
FROM
  task_4_1_1
WHERE DATE_DIFF((SELECT MAX(Last_order_date) FROM sales_info), Last_order_date, DAY) > 365
ORDER BY Total_sales DESC),
--
/*1.3 Enrich your original 1.1 SELECT by creating a new column in the view that marks active & inactive customers based on whether
 they have ordered anything during the last 365 days.
 Copy only the top 500 rows from your written select ordered by CustomerId desc */
--
task_4_1_3 AS(
  SELECT
    *,
    CASE
      WHEN DATE_DIFF(( SELECT MAX(Last_order_date) FROM sales_info), Last_order_date, DAY) > 365 THEN 'inactive'
    ELSE
    'active'
  END
    customer_activity
  FROM
    task_4_1_1
  --WHERE DATE_DIFF((SELECT MAX(Last_order_date) FROM sales_info), Last_order_date, DAY) > 365
  ORDER BY CustomerID),
--
/*1.4 Business would like to extract data on all active customers from North America. Only customers that have either ordered 2500 in total amount (with Tax) or ordered 5 + times should be presented.
In the output for these customers divide their address line into two columns, i.e.:
AddressLine1	address_no	Address_st
'8603 Elmhurst Lane'	8603	Elmhurst Lane
Order the output by country, state and date_last_order.*/
--
task_4_1_4 AS (
  SELECT
    table3.CustomerID,
    table3.FirstName,
    table3.LastName,
    table3.FullName,
    table3.addressing_title,
    table3.EmailAddress,
    table3.Phone,
    table3.AccountNumber,
    table3.CustomerType,
    table3.City,
    LEFT(table3.AddressLine1, INSTR(table3.AddressLine1, ' ' ) - 1 ) address_no,
    LTRIM(table3.AddressLine1, LEFT(table3.AddressLine1, INSTR(table3.AddressLine1, ' ' ) - 1 )) Address_st,
    table3.AddressLine2,
    table3.State,
    table3.Country,
    location_info.region,
    table3.order_amount,
    table3.Total_sales,
    table3.Last_order_date,
    table3.customer_activity
  FROM
    task_4_1_3 table3
  LEFT JOIN location_info
  ON table3.CustomerID = location_info.ID
  WHERE
    customer_activity = 'active' AND region = 'North America' AND (order_amount >= 5 OR Total_sales >= 2500)
  ORDER BY order_amount DESC, Total_sales DESC
)
--
--To see results from 4.1.1. task choose table task_4_1_1, LIMIT 200
--To see results from 4.1.2. task choose table task_4_1_2, LIMIT 200
--To see results from 4.1.3. task choose table task_4_1_3, LIMIT 500
--To see results from 4.1.4. task choose table task_4_1_4
SELECT
  *
  --, MAX(task_4_1_2.Last_order_date) OVER() Last_table_order -- for a check
FROM
  task_4_1_1
LIMIT
  200
