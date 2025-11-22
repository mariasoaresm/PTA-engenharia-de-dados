from pydantic import BaseModel
from typing import List, Dict, Any


class InputPayload(BaseModel):
    orders: List[Dict[str, Any]]
    items: List[Dict[str, Any]]
    products: List[Dict[str, Any]]
    sellers: List[Dict[str, Any]]

    class Config:
        extra = "forbid"  # campos fora do esperado vão gerar erro 422


class OutputPayload(BaseModel):
    orders: List[Dict[str, Any]]
    items: List[Dict[str, Any]]
    products: List[Dict[str, Any]]
    sellers: List[Dict[str, Any]]
    status: str = "success"
