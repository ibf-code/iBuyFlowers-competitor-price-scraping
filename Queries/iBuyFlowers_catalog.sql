WITH base AS (
  SELECT
    cb.variety,
    cb.product_group,
    CASE
     WHEN c.color_name IS NULL THEN ''
     WHEN STARTS_WITH(LOWER(c.color_name), 'dark ') THEN SPLIT(LOWER(c.color_name), ' ')[OFFSET(0)] || ' ' || SPLIT(LOWER(c.color_name), ' ')[OFFSET(1)]
     WHEN STARTS_WITH(LOWER(c.color_name), 'light ') THEN SPLIT(LOWER(c.color_name), ' ')[OFFSET(0)] || ' ' || SPLIT(LOWER(c.color_name), ' ')[OFFSET(1)]
     ELSE SPLIT(LOWER(c.color_name), ' ')[OFFSET(0)]
    END AS color_prefix,
    LOWER(v.variety_name) AS variety_name,
    LOWER(IFNULL(p.common_name, '')) AS common_name
  FROM `mart_loaded.sbx_cart_box_item` cb
  LEFT JOIN `mart_loaded.sbx_variety` v ON cb.variety = v._KEY
  LEFT JOIN `mart_loaded.sbx_product_group` p ON p._KEY = cb.product_group
  LEFT JOIN `mart_loaded.sbx_color` c ON c._KEY = v.color
  WHERE cb.variety NOT IN (
    "1c533f62-9dbc-4803-a4e8-b853120283e4", 
    "e99eb2c3-95a9-421f-ab7d-9e4f6c1659f3", 
    "b06787cf-2b7a-47e6-a1d1-bff70f8efeff",  
    "e571be69-18ac-41fb-ab74-d00edaadd4e0",  
    "0a54c97a-db7c-472a-9654-828d3c8e7a1b",  
    "64bec08c-0a46-4da1-8492-5a0cb90ecc98",  
    "66b44c18-b1be-4b61-9353-6a9c23794b97", 
    "8e8cde80-7c23-4618-b2ea-0577acc2b8cc",
    "e702c8a0-ec22-43a6-abb6-efa3585c6af4",
    "913fe526-13e3-458b-b25c-573f2fc517e1",
    "c7f33c3b-0cbb-4f63-a63a-66e502ecaaa8"
  )
  AND cb.product_group NOT IN(
    "1692521f-3547-4454-adf5-3b8bfb414633",
    "0c9f14f5-d919-4590-bbb6-f8f0f14a30e2",
    "618b7a0a-f638-4007-a05d-ba1f22fbad64",
    "492eb3d2-f8b1-4886-a180-f9fe5d4fb780",
    "5c2ebc74-f0d5-409f-83f2-a1d33372c246",
    "dc2d4e70-31a7-4009-972a-f5bbe103df9a"
    )
  LIMIT 12000
),
clean_text AS (
  SELECT
    variety,
    product_group,
    common_name,
    color_prefix,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
         CONCAT(color_prefix, ' ', variety_name, ' ', common_name),
         r'(?i)[\*\s]*fragrance[\*\s]*|(?i)\bcannot be cancelled\b',
         ''
       ),
       r'\(([^)]*\b(select|petit|petite|jumbo|italian|spray|mini|total)\b[^)]*)\)',
       r'__KEEP__\1__KEEP__'
     ), 
     r'\([^)]*\)',  
     ''
    ),
    r'__KEEP__',
    ''
    ) AS raw_phrase
 FROM base
 WHERE NOT LOWER(variety_name) LIKE '%bouquet%'
    AND NOT LOWER(variety_name) LIKE '%box%'
    AND NOT LOWER(common_name) LIKE '%bouquet%'
    AND NOT LOWER(common_name) LIKE '%box%'
    AND NOT LOWER(variety_name) LIKE '%total%'
    AND NOT LOWER(common_name) LIKE '%total%'
),
word_deduped AS (
  SELECT
    variety,
    product_group,
    common_name,
    INITCAP(
      REGEXP_REPLACE(
        TRIM(
          ARRAY_TO_STRING(ARRAY(
            SELECT DISTINCT word
            FROM UNNEST(SPLIT(raw_phrase, ' ')) AS word
            WHERE word IS NOT NULL AND word != ''
            AND NOT REGEXP_CONTAINS(word, r'\d')
          ), ' ')
        ),
        r'\s+', ' '
      )
    ) AS product_name
  FROM clean_text
)
SELECT DISTINCT
    w.product_name,
    w.variety
FROM word_deduped w
JOIN `mart_loaded.sbx_variety` v ON w.variety = v._KEY
JOIN `mart_loaded.sbx_product_group` p ON p._KEY = v.product_group