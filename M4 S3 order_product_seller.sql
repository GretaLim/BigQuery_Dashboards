-- JOIN of product data set and product category name translate tables
WITH product_info AS(
  SELECT
  products.* EXCEPT(product_category_name),
  CASE
    WHEN products.product_category_name = 'pc_gamer' THEN 'computers_games'
    WHEN products.product_category_name = 'portateis_cozinha_e_preparadores_de_alimentos' THEN 'small_appliances_kitchen_e_food_preparators'
    ELSE categories.string_field_1
  END product_category
FROM `tc-da-1.olist_db.olist_products_dataset` products
LEFT JOIN `tc-da-1.olist_db.product_category_name_translation` categories
ON products.product_category_name = categories.string_field_0),
order_details AS(
  SELECT
  order_id,
  product_id,
  seller_id,
  shipping_limit_date,
  COUNT(product_id) item_qty,
  ROUND(AVG(price),2) unit_price,
  SUM(price) total_price,
  ROUND(AVG(freight_value), 2) unit_freight_value,
  SUM(freight_value) total_freight_value
FROM `tc-da-1.olist_db.olist_order_items_dataset`
GROUP BY 1, 2, 3, 4
)
SELECT
  order_details.*,
  product_info.* EXCEPT(product_id),
  seller_state.* EXCEPT(seller_id)
  --COUNT(order_id)
FROM order_details
LEFT JOIN product_info
ON order_details.product_id = product_info.product_id
LEFT JOIN `tc-da-1.olist_db.olist_sellers_dataset` seller_state
ON order_details.seller_id = seller_state.seller_id
-- WHERE seller_state.seller_id IS NULL
LIMIT
  1000
