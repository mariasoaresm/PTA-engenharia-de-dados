from pydantic import BaseModel
from typing import List, Dict, Any, Optional

# Definimos que vamos receber listas de dicionarios
# Usamos Dict[str, Any] porque os dados brutos são sujos e não queremos
# que a API quebre se vier uma string onde deveria ser int

class InputPayload(BaseModel):
    orders: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    products: List[Dict[str, Any]] = []
    sellers: List[Dict[str, Any]] = []

class OutputPayload(BaseModel):
    orders: List[Dict[str, Any]] 
    items: List[Dict[str, Any]] 
    products: List[Dict[str, Any]]
    sellers: List[Dict[str, Any]]
    status: str = "success"