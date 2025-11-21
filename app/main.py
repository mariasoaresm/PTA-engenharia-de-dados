from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
import sys
import os

# --- CONFIGURAÇÃO DE IMPORTAÇÃO ---
# Adiciona a pasta raiz ao caminho do Python para ele conseguir importar 
# as pastas 'processing' e 'services' que estão fora da pasta 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Agora importamos o nosso ETL que criamos
from processing.main import etl_processor

app = FastAPI(
    title="Data Engineering PTA - ETL API",
    description="API que recebe dados brutos (JSON), processa DataFrames e retorna dados limpos.",
    version="1.0.0"
)

# Modelo de Entrada (Schema)
class PayloadInput(BaseModel):
    orders: Optional[List[Dict[str, Any]]] = []
    products: Optional[List[Dict[str, Any]]] = []
    order_items: Optional[List[Dict[str, Any]]] = []
    sellers: Optional[List[Dict[str, Any]]] = []

@app.get("/", tags=["Health"])
def read_root():
    return {"message": "API de Engenharia de Dados está Online! 🚀"}

@app.post("/process", tags=["ETL"], status_code=200)
def process_data(payload: PayloadInput):
    """
    Endpoint principal: Recebe JSON, limpa, valida órfãos e retorna JSON.
    """
    try:
        # Converte o input da API para dicionário Python
        raw_data = payload.dict()
        
        # Chama o nosso processador (A mágica acontece aqui)
        result = etl_processor.process_payload(raw_data)
        
        return result
    except Exception as e:
        # Se der erro, retorna mensagem clara
        raise HTTPException(status_code=500, detail=f"Erro no processamento: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)