from pydantic import BaseModel, Field
from typing import Optional, Union, Any

class Brand(BaseModel):
    name: Optional[str] = None

class Offer(BaseModel):
    lowPrice: Optional[int] = None
    highPrice: Optional[int] = None
    priceCurrency: Optional[str] = None
    price: Union[Price, int, None] = None
    availability: Optional[str] = None
    itemCondition: Optional[str] = None
    priceValidUntil: Optional[str] = None
    offerCount: Optional[int] = None

class ProductItem(BaseModel):
    item_id: Optional[str] = Field(default=None, alias="@id")
    name: Optional[str] = None
    brand: Union[Brand, str, None] = None
    image: Union[str, list, None] = None
    description: Optional[str] = None
    mpn: Optional[str] = None
    sku: Optional[str] = None
    gtin: Optional[str] = None
    offers: Optional[Offer] = None

class ScrapedResult(BaseModel):
    source_file: str
    position: Union[int, str]
    product: ProductItem
