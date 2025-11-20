from fastapi import APIRouter, HTTPException
import pandas as pd
import numpy as np
from app.schemas.payload import InputPayload, OutputPayload

# --- IMPORTAÇÃO DOS SERVIÇOS ---
try:
    # 1. Normalização de Texto e Status
    from app.services.data_normalization import (
        olist_orders_dataset, 
        olist_sellers_dataset, 
        olist_order_items_dataset
    )
    # 2. Validação de Chaves (Integridade)
    from app.services.validate_keys import tratar_registros_orfaos
    
    # 3. Tratamento de Outliers
    from app.services.adjust_outliers import tratar_outliers_iqr

except ImportError as e:
    print(f"❌ ERRO CRÍTICO DE IMPORTAÇÃO: {e}")

router = APIRouter()


# FUNÇÕES AUXILIARES ------------------------------------------------------------------------------------------------------------------

# (LÓGICA EXTRAÍDA DE temporal_cleaner.py)
def tratar_datas_completo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui o TemporalCleaner.
    Faz a conversão de datas e aplica as validações de negócio (Cronologia e Futuro).
    """
    df = df.copy()
    
    # 1. Lista de colunas de data esperadas
    date_columns = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date', 'shipping_limit_date'
    ]

    # 2. Conversão para DateTime UTC
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    # 3. Validação de Regras de Negócio (Flags de Erro)
    # Regra A: Entrega antes da compra?
    if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['dq_erro_cronologia'] = df['order_delivered_customer_date'] < df['order_purchase_timestamp']
    
    # Regra B: Compra no futuro?
    if 'order_purchase_timestamp' in df.columns:
        now_utc = pd.Timestamp.now(tz='UTC')
        df['dq_erro_futuro'] = df['order_purchase_timestamp'] > now_utc

    return df


# (LÓGICA EXTRAÍDA DE data_cleaner.py)
def tratar_produtos_completo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica regras de normalização e preenchimento de nulos para produtos.
    Baseado na lógica do data_cleaner.py.
    """
    df = df.copy()

    # 1. Padronização de Categoria
    if 'product_category_name' in df.columns:
        df['product_category_name'] = (df['product_category_name'].astype(str).str.lower().str.strip().str.replace(' ', '_').replace(['nan', 'none', ''], 'indefinido'))

    # 2. Tratamento de Nulos Numéricos (Mediana)
    cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in cols_dims:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            mediana = df[col].median()
            df[col] = df[col].fillna(mediana)
            
            # Opcional: Tratar Outliers nestas medidas (Capping)
            df = tratar_outliers_iqr(df, col, metodo='capping')

    return df


# (LÓGICA EXTRAÍDA DE )
def tratar_itens_completo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica tratamento de outliers em preços e fretes.
    """
    df = df.copy()
    
    # Preenchimento básico de nulos (se houver)
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['price'] = df['price'].fillna(df['price'].median())
        # Trata preços absurdamente altos ou baixos
        df = tratar_outliers_iqr(df, 'price', metodo='capping')
        
    if 'freight_value' in df.columns:
        df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce')
        df['freight_value'] = df['freight_value'].fillna(df['freight_value'].median())
        # Trata fretes absurdos
        df = tratar_outliers_iqr(df, 'freight_value', metodo='capping')
        
    return df


@router.post("/process", response_model=OutputPayload)
async def process_etl(payload: InputPayload):
    try:
        print("1. Recebendo dados do n8n...")

        # 1. Converter o Payload (JSON) para DataFrames do Pandas
        df_orders = pd.DataFrame(payload.orders) if payload.orders else pd.DataFrame()
        df_items = pd.DataFrame(payload.items) if payload.items else pd.DataFrame()
        df_products = pd.DataFrame(payload.products) if payload.products else pd.DataFrame()
        df_sellers = pd.DataFrame(payload.sellers) if payload.sellers else pd.DataFrame()


        # 2. ETAPA TEMPORAL (Datas)
        # Aplica conversão de datas e validação de regras de negócio (futuro, prazos)
        if not df_orders.empty:
            print("Tratando Datas em Pedidos...")
            df_orders = tratar_datas_completo(df_orders)
        
        if not df_items.empty:
            # Items também tem data: shipping_limit_date
            df_items = tratar_datas_completo(df_items)

        print(f"   - Pedidos recebidos: {len(df_orders)}")
        print(f"   - Itens recebidos: {len(df_items)}")


        # 3. ETAPA DE NORMALIZAÇÃO & LIMPEZA
        if not df_orders.empty:
            print("Normalizando Pedidos...")
            df_orders = olist_orders_dataset(df_orders)

        if not df_sellers.empty:
            print("Normalizando Vendedores...")
            df_sellers = olist_sellers_dataset(df_sellers)

        if not df_products.empty:
            print("Tratando Produtos (Nulos e Outliers)...")
            df_products = tratar_produtos_completo(df_products)

        if not df_items.empty:
            print("Tratando Itens (Preços e Outliers)...")
            df_items = olist_order_items_dataset(df_items)
            df_items = tratar_itens_completo(df_items)


        # 4. ETAPA DE INTEGRIDADE (Regra Chave)
        if not df_items.empty and (not df_orders.empty or not df_products.empty or not df_sellers.empty):
            print(f"Verificando Integridade (Itens antes: {len(df_items)})...")

            # Remove itens que apontam para pedidos/produtos/vendedores inexistentes neste lote
            # Nota: Em produção real, teríamos que validar contra o Banco de Dados também, 
            # mas aqui validamos contra o payload recebido.
            df_items = tratar_registros_orfaos(df_items, df_orders, df_products, df_sellers, acao='remover')
            print(f"Itens restantes: {len(df_items)}")

        # 5. RETORNO (DataFrame -> JSON)
        # replace({np.nan: None}) garante JSON válido
        # date_format='iso' mantém as datas legíveis para o n8n
        
        return OutputPayload(
            orders=df_orders.replace({np.nan: None}).to_dict(orient='records'),
            items=df_items.replace({np.nan: None}).to_dict(orient='records'),
            products=df_products.replace({np.nan: None}).to_dict(orient='records'),
            sellers=df_sellers.replace({np.nan: None}).to_dict(orient='records'),
            status="success"
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ ERRO NO PIPELINE: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro no processamento: {str(e)}")