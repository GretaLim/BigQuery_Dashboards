/*Use the established query to select the most expensive (price listed over 2000)
bikes that are still actively sold (does not have a sales end date).
Order the results from most to least expensive bike.*/

SELECT
  product.ProductID,
  product.Name,
  product.ProductNumber,
  product.size,
  product.color,
  product.ProductSubcategoryID,
  product.ListPrice,
  --product.SellEndDate,
  sub_category.Name AS Subcategory_name,
  category.Name AS Category_name
FROM
  `tc-da-1.adwentureworks_db.product` AS product
INNER JOIN
  `tc-da-1.adwentureworks_db.productsubcategory` AS sub_category
ON
  product.ProductSubcategoryID = sub_category.ProductSubcategoryID
LEFT JOIN
  `tc-da-1.adwentureworks_db.productcategory` AS category
ON
  sub_category.ProductCategoryID = category.ProductCategoryID
WHERE category.Name = 'Bikes' AND product.ListPrice > 2000 AND product.SellEndDate IS NULL
ORDER BY
  product.ListPrice DESC,
  sub_category.Name,
  product.ProductID;

  -- I have added more columns in the order by clause, I think such results it's easier to obser.
