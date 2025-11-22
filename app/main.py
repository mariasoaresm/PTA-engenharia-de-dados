# app/main.py
import logging
import traceback
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict, Any
import uvicorn
import os
from app.services.processor_core import etl_processor
import pandas as pd
import numpy as np


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
    orders: List[Dict[str, Any]]
    products: List[Dict[str, Any]]
    order_items: List[Dict[str, Any]]
    sellers: List[Dict[str, Any]]

    class Config:
        extra = "forbid"  # Não permite campos fora do modelo

@app.get("/", tags=["Health"])
def read_root():
    return {"message": "API de Engenharia de Dados está Online! 🚀"}

@app.post("/process", tags=["ETL"], status_code=200)
async def process_data(payload: PayloadInput, request: Request):
    try:
        raw_data = payload.model_dump()  # pydantic v2
        logger.info("Recebido payload: keys=%s", list(raw_data.keys()))

        # Executa o core ETL
        result = etl_processor.process_payload(raw_data)

        # Sanitização final contra NaN / Inf para JSON
        def sanitize_list(records):
            clean = []
            for r in records:
                clean.append({
                    k: (
                        None if (
                            isinstance(v, float)
                            and (pd.isna(v) or np.isinf(v))
                        ) else v
                    )
                    for k, v in r.items()
                })
            return clean

        if isinstance(result, dict) and "data" in result:
            for section in result.get("data", {}):
                result["data"][section] = sanitize_list(result["data"][section])

        if isinstance(result, dict) and "orphans" in result:
            for section in result.get("orphans", {}):
                result["orphans"][section] = sanitize_list(result["orphans"][section])

        return result

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Erro no endpoint /process: %s\n%s", str(e), tb)

        raise HTTPException(
            status_code=500,
            detail={"error": str(e), "traceback": tb}
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
