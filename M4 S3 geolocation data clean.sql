WITH corrected_city_name AS(
SELECT
  TRANSLATE(geolocation_city, 'çãúâéáíóüêõô.', 'cauaeaioueoo') geolocation_city_norm,
  --NORMALIZE(geolocation_city) geolocation_city_correct,
  *
FROM
  `tc-da-1.olist_db.olist_geolocation_dataset`),
final AS(
SELECT
  *,
  CASE
    WHEN geolocation_city_norm LIKE 'armacao de buzios' THEN 'armacao dos buzios'
    WHEN geolocation_city_norm LIKE 'xangri-la' THEN 'xangrila'
    WHEN geolocation_city_norm LIKE 'guajara-mirim' THEN 'guajara mirim'
    WHEN geolocation_city_norm LIKE 'alta floresta d%' THEN 'alta floresta do oeste'
    WHEN geolocation_city_norm LIKE 'alvorada d%' THEN 'alvorada do oeste'
    WHEN geolocation_city_norm LIKE 'alta alegre dos parecis' THEN 'alto alegre dos parecis'
    WHEN geolocation_city_norm LIKE 'muquem do sao francisco' THEN 'muquem de sao francisco'
    WHEN geolocation_city_norm LIKE 'mogi-mirim' THEN 'mogi mirim'
    WHEN geolocation_city_norm LIKE 'machadinho d%' THEN 'machadinho doeste'
    WHEN geolocation_city_norm LIKE 'mogi-guacu' THEN 'mogi guacu'
    WHEN geolocation_city_norm LIKE 'rio bra%' THEN 'rio branco'
    WHEN geolocation_city_norm LIKE 'sao jorge d%' THEN 'sao jorge doeste'
    WHEN geolocation_city_norm LIKE 'embu-guacu' THEN 'embuguacu'
    WHEN geolocation_city_norm LIKE 'embu das artes' THEN 'embu'
    WHEN geolocation_city_norm LIKE 'santa terezinha' THEN 'santa teresinha'
    WHEN geolocation_city_norm LIKE 'planaltina%' THEN 'planaltina'
    WHEN geolocation_city_norm LIKE 'paraty' THEN 'parati'
    WHEN geolocation_city_norm LIKE 'piumhii' THEN 'piumhi'
    WHEN geolocation_city_norm LIKE '%´%' OR geolocation_city_norm LIKE '%.%' THEN REGEXP_REPLACE(geolocation_city_norm, '[´]', '')
    ELSE geolocation_city_norm
  END city
FROM corrected_city_name),
full_table AS(
SELECT
  * EXCEPT(geolocation_lat, geolocation_lng, geolocation_city_norm),
  CONCAT(geolocation_lat, ',', geolocation_lng) geo_lat_lng,
  ROW_NUMBER () OVER (PARTITION BY geolocation_zip_code_prefix, city, geolocation_state, CONCAT(geolocation_lat, ',', geolocation_lng)) is_unique
FROM final),
unique_row_by_geopoint AS(
SELECT
  * EXCEPT(is_unique),
  ROW_NUMBER() OVER (PARTITION BY full_table.geo_lat_lng, geolocation_zip_code_prefix) point_count
FROM full_table
WHERE is_unique = 1 )
SELECT
  *
FROM unique_row_by_geopoint
LIMIT 1000

-- Code to get summary about data in this table
/*
SELECT
  COUNT(geolocation_zip_code_prefix) row_count,
  COUNT( DIStINCT geolocation_zip_code_prefix) zip_code_count,
  COUNT( DIStINCT geolocation_city) city_count,
  COUNT( DIStINCT city) city_norm_count,
  COUNT( DIStINCT geolocation_state) state_count,
  COUNT( DIStINCT geo_lat_lng) geoloc_point_count,
FROM full_table
*/
--Part of the code to check manually for duplicates in city names
/*
SELECT
  --COUNT(*)
  *
FROM unique_row_by_geopoint
WHERE unique_row_by_geopoint.geo_lat_lng IN (SELECT
  geo_lat_lng
FROM unique_row_by_geopoint
WHERE point_count > 1
)
--AND point_count = 2
ORDER BY 5, 3 */
