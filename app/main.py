# app/main.py
import logging
import traceback
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
import os
import sys
from app.services.processor_core import etl_processor
import pandas as pd
import numpy as np

# logging básico — escreve no console e em arquivo 'app.log'
LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "app.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8")
    ],
)

logger = logging.getLogger("pta-etl-api")

app = FastAPI(
    title="Data Engineering PTA - ETL API (debug)",
    description="API que recebe dados brutos (JSON), processa DataFrames e retorna dados limpos.",
    version="1.0.0"
)

class PayloadInput(BaseModel):
    orders: Optional[List[Dict[str, Any]]] = []
    products: Optional[List[Dict[str, Any]]] = []
    order_items: Optional[List[Dict[str, Any]]] = []
    sellers: Optional[List[Dict[str, Any]]] = []

@app.get("/", tags=["Health"])
def read_root():
    return {"message": "API de Engenharia de Dados está Online! 🚀"}

@app.post("/process", tags=["ETL"], status_code=200)
async def process_data(payload: PayloadInput, request: Request):
    try:
        raw_data = payload.model_dump()  # pydantic v2 compatible
        logger.info("Recebido payload: keys=%s", list(raw_data.keys()))

        # Chama o processador
        result = etl_processor.process_payload(raw_data)

        # Garantir que não há NaN/Inf nos dataframes convertidos
        # Se 'data' existir, sanitizamos (todas as listas de dicts)
        if isinstance(result, dict) and "data" in result:
            def sanitize_list_of_records(records):
                sanitized = []
                for r in records:
                    # substitui NaN/Inf por None e força serializável
                    sanitized.append({k: (None if (isinstance(v, float) and (pd.isna(v) or np.isinf(v))) else v) for k,v in r.items()})
                return sanitized

            for k in result["data"]:
                result["data"][k] = sanitize_list_of_records(result["data"][k])
            for k in result.get("orphans", {}):
                result["orphans"][k] = sanitize_list_of_records(result["orphans"][k])

        return result

    except Exception as e:
        tb = traceback.format_exc()
        # log completo
        logger.error("Erro no endpoint /process: %s\n%s", str(e), tb)
        # Retorna o traceback no body só durante debug. Remova/oculte em produção.
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})

if __name__ == "__main__":
    # Rode este comando a partir da raiz do projeto:
    # uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
