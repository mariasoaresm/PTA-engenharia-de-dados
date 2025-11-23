import logging
import traceback
import os
from typing import List, Dict, Any, Optional

import pandas as pd
import numpy as np
import uvicorn
from fastapi import FastAPI, HTTPException, Request

# --- IMPORTS DOS SERVIÇOS ---
from app.services.data_normalization import (
    limpar_pedidos,
    limpar_produtos,
    limpar_itens,
    olist_sellers_dataset
)
from app.services.validate_keys import tratar_registros_orfaos

# --- IMPORT DOS SCHEMAS ---
from app.schemas.payload import InputPayload, OutputPayload

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

# --- ENDPOINSTS ---

@app.get("/", tags=["Health"])
def read_root():
    return {"message": "API de Engenharia de Dados está Online! 🚀"}

@app.post("/process", tags=["ETL"], response_model=OutputPayload, status_code=200)
async def process_data(payload: InputPayload):
    try:
        logger.info("1. Recebendo dados para processamento...")
        
        # 1. Conversão JSON -> Pandas
        # Usamos 'if payload.x else' para garantir que não quebre se vier lista vazia
        df_orders = pd.DataFrame(payload.orders) if payload.orders else pd.DataFrame()
        df_items = pd.DataFrame(payload.items) if payload.items else pd.DataFrame()
        df_products = pd.DataFrame(payload.products) if payload.products else pd.DataFrame()
        df_sellers = pd.DataFrame(payload.sellers) if payload.sellers else pd.DataFrame()

        logger.info(f"Dados carregados: Orders={len(df_orders)}, Items={len(df_items)}, Products={len(df_products)}, Sellers={len(df_sellers)}")

        # 2. APLICAÇÃO DAS REGRAS DE NEGÓCIO
        
        # Processar Pedidos
        if not df_orders.empty:
            logger.info("Processando Pedidos...")
            df_orders = limpar_pedidos(df_orders)

        # Processar Produtos
        if not df_products.empty:
            logger.info("Processando Produtos...")
            df_products = limpar_produtos(df_products)

        # Processar Vendedores
        if not df_sellers.empty:
            logger.info("Processando Vendedores...")
            df_sellers = olist_sellers_dataset(df_sellers)

        # Processar Itens
        if not df_items.empty:
            logger.info("Processando Itens...")
            df_items = limpar_itens(df_items)

        # 3. INTEGRIDADE (Registros Órfãos)
        if not df_items.empty and (not df_orders.empty or not df_products.empty or not df_sellers.empty):
            logger.info("🔗 Verificando Integridade (Removendo Órfãos)...")
            df_items = tratar_registros_orfaos(
                df_items, df_orders, df_products, df_sellers, acao='remover'
            )

        # 4. PREPARAÇÃO DO RETORNO
        def to_dict_clean(df: pd.DataFrame) -> List[Dict[str, Any]]:
            if df.empty:
                return []
            return df.replace({np.nan: None}).to_dict(orient='records')

        logger.info("Processamento concluído. Preparando resposta.")

        # 5. RETORNO (Usando o OutputPayload importado)
        return OutputPayload(
            orders=to_dict_clean(df_orders),
            items=to_dict_clean(df_items),
            products=to_dict_clean(df_products),
            sellers=to_dict_clean(df_sellers),
            status="success"
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Erro Crítico no processamento ETL: %s\n%s", str(e), tb)
        
        raise HTTPException(
            status_code=500, 
            detail={"error": str(e), "traceback": tb.splitlines()}
        )
