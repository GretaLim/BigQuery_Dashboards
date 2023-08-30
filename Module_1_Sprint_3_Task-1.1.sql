/* You’ve been asked to extract the data on products from the Product table where there exists a product subcategory.
And also include the name of the ProductSubcategory.
Columns needed: ProductId, Name, ProductNumber, size, color, ProductSubcategoryId, Subcategory name.
Order results by SubCategory name.
*/

SELECT
  product.ProductID,
  product.Name,
  product.ProductNumber,
  product.size,
  product.color,
  product.ProductSubcategoryID,
  subprod.Name Subproduct_name
FROM
  `tc-da-1.adwentureworks_db.product` AS product
INNER JOIN
  `tc-da-1.adwentureworks_db.productsubcategory` AS subprod
ON
  product.ProductSubcategoryID = subprod.ProductSubcategoryID
ORDER BY
  subprod.Name,
  product.ProductID;
