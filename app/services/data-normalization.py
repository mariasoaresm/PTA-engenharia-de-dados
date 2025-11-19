import pandas as pd

MAPA_STATUS_PEDIDOS = {
    'delivered': 'entregue',
    'shipped': 'enviado',
    'canceled': 'cancelado',
    'unavailable': 'indisponivel',
    'invoiced': 'faturado',
    'processing': 'processando',
    'created': 'criado',
    'approved': 'aprovado'
}

def olist_orders_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tratamento da tabela de PEDIDOS (Orders).
    """
    df = df.copy()

    # 1. IDs para String
    columns_id = ['order_id', 'customer_id', 'order_status']
    for column in columns_id:
        df[column] = df[column].astype(str)

    # 2. Padronização e Tradução de Status
    df['order_status'] = df['order_status'].str.lower().str.strip()
    
    df['order_status_traduzido'] = df['order_status'].map(MAPA_STATUS_PEDIDOS)
    df['order_status'] = df['order_status_traduzido'].fillna(df['order_status'])
    
    df = df.drop(columns=['order_status_traduzido'])
    df['order_status'] = df['order_status'].astype('category')

    # 3. Datas
    columns_datas = [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]

    for column in columns_datas:
        df[column] = pd.to_datetime(df[column], errors='coerce') 

    return df


def olist_products_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tratamento da tabela de PRODUTOS (Products).
    """
    df = df.copy()
    
    # 1. Garantia de Tipos (IDs)
    df['product_id'] = df['product_id'].astype(str)

    # 2. Tratamento de Categoria (Texto)
    df['product_category_name'] = df['product_category_name'].fillna('indefinido')
    
    # Aplica transformações: String -> Lower -> Espaço por Underscore -> Category
    df['product_category_name'] = (
        df['product_category_name']
        .astype(str)
        .str.lower()
        .str.replace(' ', '_')
        .astype('category')
    )

    return df


def olist_sellers_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tratamento da tabela de VENDEDORES (Sellers).
    """
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


def olist_order_items_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tratamento da tabela de ITENS PEDIDOS (Order itens).
    """
    df = df.copy()

    # 1. Garantia de Tipos (IDs)
    columns_id = ['order_id', 'product_id', 'seller_id']
    for column in columns_id:
        df[column] = df[column].astype(str)

    # 2. Conversão Numérica
    colums_float = ['price', 'freight_value']
    for column in colums_float:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    # 3. Conversão de Datas
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')

    return df