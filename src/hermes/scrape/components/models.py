from __future__ import annotations
from typing import List, Optional, Union, Any
from pydantic import BaseModel, Field, HttpUrl, validator

class Brand(BaseModel):
    type: str = Field(alias="@type", default="Brand")
    name: Optional[str] = None

class Seller(BaseModel):
    type: str = Field(alias="@type", default="Organization")
    name: Optional[str] = None

class Offer(BaseModel):
    type: str = Field(alias="@type", default="Offer")
    price: Optional[int] = None
    priceCurrency: Optional[str] = None
    availability: Optional[str] = None
    sku: Optional[str] = None
    itemCondition: Optional[str] = None
    priceValidUntil: Optional[str] = None
    seller: Optional[Seller] = None

class AggregateOffer(BaseModel):
    type: str = Field(alias="@type", default="AggregateOffer")
    lowPrice: Optional[int] = None
    highPrice: Optional[int] = None
    priceCurrency: Optional[str] = None
    offerCount: Optional[int] = None
    # The 'offers' field inside AggregateOffer is a list of individual Offer objects
    offers: List[Offer] = Field(default_factory=list)

class ProductItem(BaseModel):
    context: Optional[str] = Field(alias="@context", default=None)
    type: str = Field(alias="@type", default="Product")
    id: Optional[str] = Field(alias="@id", default=None)
    name: Optional[str] = None
    brand: Optional[Brand] = None
    image: Optional[str] = None
    description: Optional[str] = None
    mpn: Optional[str] = None
    sku: Optional[str] = None
    offers: Union[Offer, AggregateOffer, None] = None

class ScrapedResult(BaseModel):
    source_file: str
    position: Union[int, str]
    product: ProductItem

# --- Usage Example ---
# if __name__ == "__main__":
#     raw_data = {
#         '@context': 'https://schema.org/',
#         '@type': 'Product',
#         '@id': 'https://www.carrefour.com.ar/zapallo-anco-x-kg/p',
#         'name': 'Zapallo anco x kg.',
#         'brand': {'@type': 'Brand', 'name': 'Genérico'},
#         'image': 'https://carrefourar.vtexassets.com/arquivos/ids/799826/2304651000005_02.jpg?v=639064288324630000',
#         'description': '',
#         'mpn': '2304651000005',
#         'sku': '8169',
#         'offers': {
#             '@type': 'AggregateOffer',
#             'lowPrice': 999,
#             'highPrice': 999,
#             'priceCurrency': 'ARS',
#             'offers': [{
#                 '@type': 'Offer',
#                 'price': 999,
#                 'priceCurrency': 'ARS',
#                 'availability': 'http://schema.org/InStock',
#                 'sku': '8169',
#                 'itemCondition': 'http://schema.org/NewCondition',
#                 'priceValidUntil': '2026-02-24T03:00:00Z',
#                 'seller': {'@type': 'Organization', 'name': 'CARREFOUR'}
#             }],
#             'offerCount': 1
#         }
#     }
#
#     try:
#         product = Product(**raw_data)
#         print(f"Product Parsed: {product.name}")
#         print(f"Brand: {product.brand.name}")
#         print(f"Lowest Price: {product.offers.lowPrice} {product.offers.priceCurrency}")
#         print(f"Seller: {product.offers.offers[0].seller.name}")
#     except Exception as e:
#         print(f"Validation Error: {e}")
#
