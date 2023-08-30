/*In 1.1 query you have a product subcategory but see that you could use the category name.
Find and add the product category name.
Afterwards order the results by Category name.*/

SELECT
  product.ProductID,
  product.Name,
  product.ProductNumber,
  product.size,
  product.color,
  product.ProductSubcategoryID,
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
ORDER BY
  category.Name,
  sub_category.Name,
  product.ProductID;
  -- I have added more columns in the order by clause, I think such results it's easier to obser.
