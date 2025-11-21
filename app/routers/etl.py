from fastapi import APIRouter, HTTPException
import pandas as pd
import numpy as np
from app.schemas.payload import InputPayload, OutputPayload

# Importação Direta das Funções de Limpeza Principais
from app.services.data_normalization import (
    limpar_pedidos,
    limpar_produtos,
    limpar_itens,
    olist_sellers_dataset
)
from app.services.validate_keys import tratar_registros_orfaos

router = APIRouter()

# --- ENDPOINT PRINCIPAL ---

@router.post("/process", response_model=OutputPayload)
async def process_etl(payload: InputPayload):
    try:
        print("1. Recebendo dados...")

        # 1. Conversão JSON -> Pandas
        # Garante a criação de DataFrames vazios caso o payload venha nulo
        df_orders = pd.DataFrame(payload.orders) if payload.orders else pd.DataFrame()
        df_items = pd.DataFrame(payload.items) if payload.items else pd.DataFrame()
        df_products = pd.DataFrame(payload.products) if payload.products else pd.DataFrame()
        df_sellers = pd.DataFrame(payload.sellers) if payload.sellers else pd.DataFrame()

        # 2. APLICAÇÃO DAS REGRAS DE NEGÓCIO (Chamadas diretas)
        
        if not df_orders.empty:
            print(" Processando Pedidos...")
            df_orders = limpar_pedidos(df_orders)

        if not df_products.empty:
            print(" Processando Produtos...")
            df_products = limpar_produtos(df_products)

        if not df_sellers.empty:
            print(" Processando Vendedores...")
            df_sellers = olist_sellers_dataset(df_sellers)

        if not df_items.empty:
            print(" Processando Itens...")
            df_items = limpar_itens(df_items)

        # 3. INTEGRIDADE
        if not df_items.empty and (not df_orders.empty or not df_products.empty or not df_sellers.empty):
            print("   Verificando Integridade...")
            df_items = tratar_registros_orfaos(
                df_items, df_orders, df_products, df_sellers, acao='remover'
            )

        # 4. SANITIZAÇÃO FINAL (Correção solicitada pela Gerência)
        # Substitui explicitamente np.nan por None para garantir conformidade com JSON (null)
        # Isso é feito antes da conversão para dicionário para evitar erros de validação do Pydantic
        print("4. Sanitizando dados para saída...")
        
        df_orders = df_orders.replace({np.nan: None})
        df_items = df_items.replace({np.nan: None})
        df_products = df_products.replace({np.nan: None})
        df_sellers = df_sellers.replace({np.nan: None})

        # 5. RETORNO
        # O retorno agora apenas converte os DataFrames já limpos
        return OutputPayload(
            orders=df_orders.to_dict(orient='records'),
            items=df_items.to_dict(orient='records'),
            products=df_products.to_dict(orient='records'),
            sellers=df_sellers.to_dict(orient='records'),
            status="success"
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        # Retorna o erro detalhado mantendo o status 500
        raise HTTPException(status_code=500, detail=f"Erro no processamento: {str(e)}")