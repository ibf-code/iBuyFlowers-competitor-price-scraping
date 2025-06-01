import re
from Cleaners.cleaner import clean_product_name, extract_stem_length, extract_stems_per_unit
from schemas.product_schema import ProductSchema

def extract_dvflora_stem_length(raw_product_name):
    if not raw_product_name:
        return None
    
    merged_match = re.search(r'\b(\d{2})(\d{2})\b', raw_product_name)
    if merged_match:
        return int(merged_match.group(1)) 
    
    range_match = re.search(r'\b(\d+)[-/](\d+)\b', raw_product_name)
    if range_match:
        return int(range_match.group(1))
    
    return None

def extract_dvflora_product(raw):
    product_name_raw = raw.get("product_name", "")
    if product_name_raw:
        product_name_raw = re.sub(r'\s+[A-Z]{2}$', '', product_name_raw)
        product_name_raw = re.sub(r'\s+[A-Z]{2}\s+', ' ', product_name_raw)
    
    cleaned_name = clean_product_name(product_name_raw, strategy="aggressive")
    stem_length = extract_stem_length(product_name_raw)
    if not stem_length:
        stem_length = extract_dvflora_stem_length(product_name_raw)

    product = ProductSchema(
        competitor="DVFlora",
        competitor_product_id=str(raw.get("item_number")) if raw.get("item_number") else None,
        competitor_product_name=cleaned_name,
        competitor_product_group_name=raw.get("product_group"),
        unit=raw.get("sold_as"),
        color=raw.get("color"),
        stem_length=stem_length,
        stems_per_unit=extract_stems_per_unit(product_name_raw),
        unit_price=raw.get("unit_price"),
        stem_price=raw.get("best_value_price"),
        unit_price_fallback=raw.get("unit_price_extracted"), 
        available_units=raw.get("best_value_quantity"),
        grower_country=raw.get("origin"),
        cleaned_name_used=cleaned_name,
    )
    return product.dict()