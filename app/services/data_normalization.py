import pandas as pd
# Importamos a função de outliers para usar dentro da limpeza de produtos/itens
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
    """
    Lógica pesada de transformação para PEDIDOS:
    1. Datas (conversão).
    2. Status (tradução).
    3. Métricas (atraso, prazo).
    """
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
            
    # Validação de Regras de Negócio (Datas)
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
    if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['tempo_entrega_dias'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days

    if 'order_estimated_delivery_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['tempo_entrega_estimado_dias'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.days

    # Lógica Estrita do Desafio: Sim, Não, Não Entregue
    def verificar_prazo(row):
        # 1. Se não tem data de entrega real -> "Não Entregue"
        if pd.isnull(row.get('order_delivered_customer_date')):
            return 'Não Entregue'

        # 2. Se Entrega Real <= Estimada -> "Sim" (No prazo)
        if row['order_delivered_customer_date'] <= row['order_estimated_delivery_date']:
            return 'Sim'
        else:
            # 3. Caso contrário (Atrasado ou data estimada nula) -> "Não"
            return 'Não'

    if 'order_delivered_customer_date' in df.columns and 'order_estimated_delivery_date' in df.columns:
        df['entrega_no_prazo'] = df.apply(verificar_prazo, axis=1)

    return df


def limpar_produtos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lógica pesada para PRODUTOS:
    1. Texto (categoria).
    2. Estatística (mediana e outliers).
    """
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
            mediana = df[col].median()
            df[col] = df[col].fillna(mediana)
            df = tratar_outliers_iqr(df, col, metodo='capping')

    return df


def limpar_itens(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lógica para ITENS:
    1. Datas básicas.
    2. Estatística de preços (mediana/outliers).
    """
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
            df[col] = df[col].fillna(df[col].median())
            df = tratar_outliers_iqr(df, col, metodo='capping')
            
    return df


# Wrappers para compatibilidade
def olist_orders_dataset(df): return limpar_pedidos(df)
def olist_products_dataset(df): return limpar_produtos(df)
def olist_order_items_dataset(df): return limpar_itens(df)


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

