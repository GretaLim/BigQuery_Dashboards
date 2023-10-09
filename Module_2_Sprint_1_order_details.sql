SELECT
  salesdetail.SalesOrderID,
  salesdetail.OrderQty,
  salesdetail.ProductID,
  salesdetail.LineTotal,
  product.Name ProductName,
  salesheader.OrderDate,
    CASE
    WHEN subcategory.Name IS NULL THEN product.Name
    ELSE subcategory.Name
  END subcategory
From `adwentureworks_db.salesorderdetail` salesdetail
JOIN `adwentureworks_db.salesorderheader` salesheader
ON salesdetail.SalesOrderID = salesheader.SalesOrderID AND SalesPersonID IS NULL
LEFT JOIN `adwentureworks_db.product` product
ON product.ProductID = salesdetail.ProductID
LEFT JOIN
  `adwentureworks_db.productsubcategory` subcategory
ON product.ProductSubcategoryID = subcategory.ProductSubcategoryID
