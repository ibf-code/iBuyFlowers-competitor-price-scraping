import csv
import os
from schemas.product_schema import ProductSchema

def save_products_to_csv(products, output_path):
    if not products:
        print("❌ No products to save.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Convert ProductSchema objects to dictionaries if needed
    if products and hasattr(products[0], 'model_dump'):
        products_dicts = [p.model_dump() for p in products]
    else:
        products_dicts = products

    # Only include fields from schema that are used (non-empty) at least once
    schema_fields = list(ProductSchema.model_fields.keys())
    fieldnames = [field for field in schema_fields if any(p.get(field) not in [None, "", [], {}] for p in products_dicts)]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for product in products_dicts:
            writer.writerow({k: product.get(k, "") for k in fieldnames})

    # print(f"✅ Saved {len(products)} rows to {output_path} with {len(fieldnames)} columns")