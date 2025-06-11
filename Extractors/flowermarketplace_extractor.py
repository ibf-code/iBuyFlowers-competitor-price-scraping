import re
from Cleaners.cleaner import clean_product_name, extract_stem_length, extract_stems_per_unit
from schemas.product_schema import ProductSchema

def extract_flowermarketplace_product(raw_product):
    product_name_raw = raw_product.get('name', '') or ''
    cleaned_name = clean_product_name(product_name_raw, strategy="aggressive")
    stem_price = None
    prices_data = raw_product.get("prices_data", [])

    for selection in ["one", "three"]:  ## Defeault is Pricing one ( highest price) , fallback is option three cheapest
        for tier in prices_data:
            if tier.get("selection") == selection:
                try:
                    stem_price = float(tier.get("landed_price"))
                    break
                except (ValueError, TypeError):
                    continue
        if stem_price is not None:
            break

    unit_price = raw_product.get("unit_price")
    stems_per_unit = extract_stems_per_unit(product_name_raw) or 1

    if stem_price is None and unit_price:
        stem_price = float(unit_price)

    if stem_price is not None and stems_per_unit > 1:
        stem_price = round(stem_price / stems_per_unit, 2)

    if stem_price is None:
        print(f"⚠️ Missing stem_price for product {raw_product.get('id')} - Name: {product_name_raw}")

    product = ProductSchema(
        competitor="FlowerMarketplace",
        competitor_product_id=str(raw_product.get("id")) if raw_product.get("id") else None,
        competitor_product_name=cleaned_name,
        competitor_product_group_name=(raw_product.get("cat") or "").strip(),
        unit=raw_product.get("unit"),
        stem_length=extract_stem_length(product_name_raw),
        stems_per_unit=stems_per_unit,
        stem_price=stem_price,
        grower_country=raw_product.get("grower_country"),
    )
    return product.dict()