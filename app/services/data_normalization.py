import pandas as pd
import numpy as np
from app.services.adjust_outliers import tratar_outliers_iqr

# Dicionário de tradução de status conforme requisito
MAPA_STATUS_PEDIDOS = {
    'delivered': 'entregue',
    'invoiced': 'faturado',
    'shipped': 'enviado',
    'processing': 'em processamento',
    'unavailable': 'indisponível',
    'canceled': 'cancelado',
    'created': 'criado',
    'approved': 'aprovado'
}


def limpar_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    df = df.copy()

    # 1. Conversão de Datas
    cols_datas = [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    for col in cols_datas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            
    # Validação de Regras de Negócio (Colunas DQ - Comentadas na análise de erro, mas mantidas)
    if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['dq_erro_cronologia'] = df['order_delivered_customer_date'] < df['order_purchase_timestamp']
    
    if 'order_purchase_timestamp' in df.columns:
        now_utc = pd.Timestamp.now(tz='UTC')
        df['dq_erro_futuro'] = df['order_purchase_timestamp'] > now_utc

    # 2. Tradução de Status
    if 'order_status' in df.columns:
        df['order_status'] = df['order_status'].astype(str).str.lower().str.strip()
        df['order_status'] = df['order_status'].map(MAPA_STATUS_PEDIDOS).fillna(df['order_status'])

    # 3. Engenharia de Atributos (Métricas de Tempo)
    # COMENTE ESTES BLOCOS SE O ERRO 500 PERSISTIR AQUI!
    if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['tempo_entrega_dias'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days

    if 'order_estimated_delivery_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['tempo_entrega_estimado_dias'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.days

    # Lógica Estrita do Desafio: Sim, Não, Não Entregue (CORREÇÃO DE LÓGICA DE NULL)
    def verificar_prazo(row):
        data_entrega = row.get('order_delivered_customer_date')
        data_estimada = row.get('order_estimated_delivery_date')

        # 1. Se a data de entrega real é nula/NaT
        if pd.isnull(data_entrega):
            return 'Não Entregue'

        # 2. Se a entrega ocorreu, mas a data estimada é nula
        if pd.isnull(data_estimada):
            return 'Não'
        
        # 3. Faz a comparação APENAS se ambas as datas são válidas
        if data_entrega <= data_estimada:
            return 'Sim'
        else:
            return 'Não'

    if 'order_delivered_customer_date' in df.columns and 'order_estimated_delivery_date' in df.columns:
        df['entrega_no_prazo'] = df.apply(verificar_prazo, axis=1)

    return df


def limpar_produtos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
        
    df = df.copy()

    # 1. Categoria (Texto)
    if 'product_category_name' in df.columns:
        df['product_category_name'] = (
            df['product_category_name']
            .astype(str).str.lower().str.strip()
            .str.replace(' ', '_')
            .replace(['nan', 'none', ''], 'indefinido')
        )

    # 2. Estatística (Mediana e Outliers)
    cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in cols_dims:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

            # CORREÇÃO: REGRA DE SEGURANÇA para mediana (evita NaN no fillna)
            mediana = df[col].median()
            mediana_segura = mediana if not pd.isna(mediana) else 0

            df[col] = df[col].fillna(mediana_segura)
            df = tratar_outliers_iqr(df, col, metodo='capping')

    return df


def limpar_itens(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
        
    df = df.copy()
    
    # Datas
    if 'shipping_limit_date' in df.columns:
        df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce', utc=True)
        
    # Estatística
    for col in ['price', 'freight_value']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

            # CORREÇÃO: REGRA DE SEGURANÇA para mediana (evita NaN no fillna)
            mediana = df[col].median()
            mediana_segura = mediana if not pd.isna(mediana) else 0

            df[col] = df[col].fillna(mediana_segura)
            df = tratar_outliers_iqr(df, col, metodo='capping')
            
    return df


# Wrappers para compatibilidade
def olist_orders_dataset(df): return limpar_pedidos(df)
def olist_products_dataset(df): return limpar_produtos(df)
def olist_order_items_dataset(df): return limpar_itens(df)


def olist_sellers_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Garantia de Tipos (String)
    columns_str = ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']
    
    for col in columns_str:
        df[col] = df[col].astype(str)

    # 2. Regra: Remover Acentos (Normalização) da Cidade
    df['seller_city'] = (
        df['seller_city']
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
    )

    # 3. Regra: Uppercase
    df['seller_city'] = df['seller_city'].str.upper().str.strip()
    df['seller_state'] = df['seller_state'].str.upper().str.strip()

    return df