WITH sales_per AS(
  SELECT
    sales_info.SalesOrderID OrderID,
    sales_info.TotalDue Total_sales,
    round(SUM(sales_info.TotalDue) OVER (PARTITION BY sales_info.SalesOrderID) /(SELECT SUM(TotalDue) FROM `adwentureworks_db.salesorderheader`), 6) partinsales
  FROM
    `adwentureworks_db.salesorderheader` AS sales_info
  GROUP BY sales_info.SalesOrderID, sales_info.TotalDue
),
--
sales_person_info AS(
  SELECT
  salesperson.SalesPersonID as ID,
  employee.Title AS Title,
  CONCAT(contact.Firstname," ",contact.LastName) SalesPersonName
FROM
  `tc-da-1.adwentureworks_db.salesperson` AS salesperson
JOIN `adwentureworks_db.employee` AS employee
ON employee.EmployeeId = salesperson.SalesPersonID
JOIN `adwentureworks_db.contact` AS contact
ON contact.ContactId = employee.ContactID)
--
SELECT
  salesorderheader.* EXCEPT(Status, Comment, rowguid, ModifiedDate, SalesPersonID),
  CAST(DATE_TRUNC(salesorderheader.OrderDate, month) AS Date) AS Order_Date,
  province.stateprovincecode as ship_province,
  province.CountryRegionCode as country_code,
  province.name as country_state_name,
  territory.name AS region_name,
  territory.Group AS territory_group,
  sales_per.partinsales,
  sales_person_info.Title,
  sales_person_info.SalesPersonName
FROM `tc-da-1.adwentureworks_db.salesorderheader` as salesorderheader
INNER JOIN
`tc-da-1.adwentureworks_db.address` as address
on salesorderheader.ShipToAddressID = address.AddressID
INNER JOIN
`tc-da-1.adwentureworks_db.stateprovince` as province
ON address.StateProvinceID = province.StateProvinceID
INNER JOIN
`adwentureworks_db.salesterritory` as territory
on territory.TerritoryID = salesorderheader.TerritoryID
LEFT JOIN sales_per
ON sales_per.OrderID = salesorderheader.SalesOrderID
LEFT JOIN sales_person_info
ON sales_person_info.ID = salesorderheader.SalesOrderID
