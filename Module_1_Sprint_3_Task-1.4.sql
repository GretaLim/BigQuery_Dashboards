-- Your colleague has written a query to find the highest sales connected to special offers. The query works fine but the numbers are off, investigate where the potential issue lies.

SELECT
  sales_detail.SalesOrderId,
  sales_detail.OrderQty,
  ROUND(sales_detail.UnitPrice, 2) UnitPrice,
  ROUND( sales_detail.LineTotal,2) LineTotal,
  --sales_detail.UnitPriceDiscount,
  sales_detail.ProductId,
  sales_detail.SpecialOfferID,
  spec_offer_product.rowguid,
  spec_offer_product.ModifiedDate,
  spec_offer.Category,
  spec_offer.Description
FROM
  tc-da-1.adwentureworks_db.salesorderdetail AS sales_detail
LEFT JOIN
  tc-da-1.adwentureworks_db.specialofferproduct AS spec_offer_product
  -- additional condition needed 'AND sales_detail.SpecialOfferID = spec_offer_product.SpecialOfferID'
ON
  sales_detail.productId = spec_offer_product.ProductID
  AND sales_detail.SpecialOfferID = spec_offer_product.SpecialOfferID
LEFT JOIN
  tc-da-1.adwentureworks_db.specialoffer AS spec_offer
ON
  sales_detail.SpecialOfferID = spec_offer.SpecialOfferID
  -- SpecialOfferID 1 means, what there wasn't any discounts made for an offer
WHERE
  sales_detail.SpecialOfferID != 1 AND sales_detail.UnitPriceDiscount != 0
ORDER BY
  LineTotal desc
