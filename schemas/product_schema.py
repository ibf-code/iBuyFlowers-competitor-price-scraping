from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class ProductSchema(BaseModel):
    competitor: str
    competitor_product_id: str
    competitor_product_name: Optional[str] = None
    competitor_variant_id: Optional[str] = None
    competitor_product_group_name: Optional[str] = None
    competitor_product_group_id: Optional[str] = None
    unit: Optional[str] = None
    state: Optional[str] = None
    color: Optional[str] = None
    stem_length: Optional[int] = None
    stems_per_unit: Optional[int] = None
    unit_price: Optional[float] = None
    stem_price: Optional[float] = None
    base_price: Optional[float] = None
    freight_price: Optional[float] = None
    margin: Optional[float] = None
    available_units: Optional[int] = None
    grower_name: Optional[str] = None
    grower_country: Optional[str] = None
    highlight_name: Optional[str] = None
    product_url: Optional[str] = None
    image_url: Optional[str] = None
    matched_variety_name: Optional[str] = None
    variety_key: Optional[str] = None
    variety_score: Optional[float] = None
    cleaned_name_used: Optional[str] = None
    matched_product_group_name: Optional[str] = None
    product_group_key: Optional[str] = None
    product_group_score: Optional[float] = None
    cheapest_price_per_stem: Optional[float] = None  
    highest_price_per_stem: Optional[float] = None  
    created_at: str = Field(default_factory=lambda: datetime.utcnow().strftime('%Y-%m-%d'))
    eta_date: Optional[str] = None