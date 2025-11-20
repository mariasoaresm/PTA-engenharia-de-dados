from fastapi import APIRouter, HTTPException
import pandas as pd
import numpy as np
from app.schemas.payload import InputPayload, OutputPayload

# --- IMPORTAÇÃO DOS SERVIÇOS ---
from app.services.data_normalization import (
    olist_orders_dataset, 
    olist_sellers_dataset, 
    olist_order_items_dataset,
    olist_products_dataset
)
from app.services.validate_keys import tratar_registros_orfaos
from app.services.adjust_outliers import tratar_outliers_iqr

router = APIRouter()

# --- FUNÇÕES AUXILIARES LOCAIS ---

def tratar_datas_completo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui o TemporalCleaner. Faz conversão e validação de datas.
    """
    df = df.copy()
    date_columns = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date', 'shipping_limit_date'
    ]
    
    # Conversão
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    # Regras de Negócio
    if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['dq_erro_cronologia'] = df['order_delivered_customer_date'] < df['order_purchase_timestamp']
    
    if 'order_purchase_timestamp' in df.columns:
        now_utc = pd.Timestamp.now(tz='UTC')
        df['dq_erro_futuro'] = df['order_purchase_timestamp'] > now_utc

    return df

def enrich_products_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apenas Estatística (Mediana e Outliers) para Produtos.
    A normalização de texto é feita pelo service 'olist_products_dataset'.
    """
    df = df.copy()
    
    cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in cols_dims:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            mediana = df[col].median()
            df[col] = df[col].fillna(mediana)
            df = tratar_outliers_iqr(df, col, metodo='capping')
    return df

def tratar_itens_completo(df: pd.DataFrame) -> pd.DataFrame:
    """Estatística para Itens."""
    df = df.copy()
    for col in ['price', 'freight_value']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(df[col].median())
            df = tratar_outliers_iqr(df, col, metodo='capping')
    return df

# --- ENDPOINT PRINCIPAL ---

@router.post("/process", response_model=OutputPayload)
async def process_etl(payload: InputPayload):
    try:
        print("1. Recebendo dados do n8n...")

        # 1. Conversão JSON -> Pandas
        df_orders = pd.DataFrame(payload.orders) if payload.orders else pd.DataFrame()
        df_items = pd.DataFrame(payload.items) if payload.items else pd.DataFrame()
        df_products = pd.DataFrame(payload.products) if payload.products else pd.DataFrame()
        df_sellers = pd.DataFrame(payload.sellers) if payload.sellers else pd.DataFrame()

        # 2. TRATAMENTO TEMPORAL
        if not df_orders.empty:
            df_orders = tratar_datas_completo(df_orders)
        if not df_items.empty:
            df_items = tratar_datas_completo(df_items)

        # 3. NORMALIZAÇÃO & LIMPEZA
        if not df_orders.empty:
            df_orders = olist_orders_dataset(df_orders)

        if not df_sellers.empty:
            df_sellers = olist_sellers_dataset(df_sellers)

        if not df_products.empty:
            # 1. Normalização de Texto (Service)
            df_products = olist_products_dataset(df_products)
            # 2. Estatística (Local)
            df_products = enrich_products_stats(df_products)

        if not df_items.empty:
            df_items = olist_order_items_dataset(df_items)
            df_items = tratar_itens_completo(df_items)

        # 4. INTEGRIDADE
        if not df_items.empty and (not df_orders.empty or not df_products.empty or not df_sellers.empty):
            df_items = tratar_registros_orfaos(
                df_items, df_orders, df_products, df_sellers, acao='remover'
            )

        # 5. RETORNO
        output = OutputPayload(
            orders=df_orders.replace({np.nan: None}).to_dict(orient='records'),
            items=df_items.replace({np.nan: None}).to_dict(orient='records'),
            products=df_products.replace({np.nan: None}).to_dict(orient='records'),
            sellers=df_sellers.replace({np.nan: None}).to_dict(orient='records'),
            status="success"
        )
        
        return output

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Erro no processamento: {str(e)}")