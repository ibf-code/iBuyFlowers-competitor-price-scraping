from Cleaners.cleaner import clean_product_name, extract_stem_length, extract_stems_per_unit
from schemas.product_schema import ProductSchema
from datetime import datetime

def extract_mayesh_product(raw):
    name = raw.get("name", "") or ""
    cleaned_name = clean_product_name(name, strategy="aggressive")
    product = ProductSchema(
        competitor="Mayesh",
        competitor_product_id=str(raw.get("product_id")) if raw.get("product_id") else None,
        competitor_product_name=cleaned_name,
        competitor_variant_id=None,  # Mayesh doesn’t provide this
        competitor_product_group_name=raw.get("category_name", "").strip(),
        competitor_product_group_id=str(raw.get("category_id")) if raw.get("category_id") else None,
        unit=raw.get("unit_type"),
        color=(raw.get("color_name") or "").strip(),
        stem_length=extract_stem_length(name) or extract_stem_length(raw.get("grade_name")),
        stems_per_unit=raw.get("unit_count") or extract_stems_per_unit(name),
        unit_price=raw.get("price_per_unit"),
        stem_price=raw.get("price_per_stem"),
        base_price=raw.get("main_landed_cost"),
        freight_price=raw.get("freight"),
        margin=(1 - (1 / raw["markup"])) * 100 if raw.get("markup") else None,
        available_units=raw.get("qty"),
        grower_name=raw.get("farm_name"),
        grower_country=raw.get("country_name"),
        highlight_name=raw.get("highlight_name"),
        product_url=f"https://www.mayesh.com/{raw.get('seo_url')}" if raw.get("seo_url") else None,
        image_url=None,  # Optional, Mayesh may not provide
        matched_variety_name=None,
        variety_key=None,
        variety_score=None,
        cleaned_name_used=cleaned_name,
        matched_product_group_name=None,
        product_group_key=str(raw.get("category_id")) if raw.get("category_id") else None,
        product_group_score=None,
        cheapest_price_per_stem=None,
        highest_price_per_stem=None,
        created_at=datetime.utcnow().strftime('%Y-%m-%d'),
        eta_date=None,
        state="Kentucky"
    )
    return product
