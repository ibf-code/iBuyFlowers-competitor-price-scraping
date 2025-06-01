import re
from Cleaners.cleaner import clean_product_name, extract_stem_length, extract_stems_per_unit
from schemas.product_schema import ProductSchema    

def extract_price_per_stem_from_title(text):
    if not text:
        return None

    match = re.search(r'\(\s*\$(\d+\.\d{2})\s*Each\s*\)', text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def extract_petaljet_product(raw_product):
    product_title = raw_product.get("title", "").strip()
    product_type = raw_product.get("product_type", "").strip()
    product_id = raw_product.get("id")
    variants = raw_product.get("variants", [])
    tags = raw_product.get("tags", [])
    color = tags[0].strip() if tags else ""

    extracted = []

    for variant in variants:
            variant_title = variant.get("title", "").strip()
            cleaned_name = re.sub(r'\(\$\d+\.\d{2}\s*Each\)', '', product_title, flags=re.IGNORECASE).strip()
            cleaned_name = clean_product_name(cleaned_name)
            stems_per_unit = extract_stems_per_unit(variant_title)
            stem_length = extract_stem_length(variant_title)
            unit_price = float(variant.get("price", 0)) if variant.get("price") else None
            stem_price = extract_price_per_stem_from_title(variant_title)
            
            if not stem_price and unit_price and stems_per_unit:
                stem_price = round(unit_price / stems_per_unit, 2)
            
            product = ProductSchema(
                competitor="PetalJet",
                competitor_product_id=str(product_id) if product_id else None,
                competitor_product_name=cleaned_name,
                competitor_product_group_name=product_type,
                color=color,
                stem_length=stem_length,
                stems_per_unit=stems_per_unit,
                unit_price=unit_price,
                stem_price=stem_price,
                cleaned_name_used=cleaned_name,
            )

            extracted.append(product.dict())
    return extracted
