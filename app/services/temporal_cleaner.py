import pandas as pd
import numpy as np

# Colunas que sempre tentaremos converter para data
DATE_COLUMNS = [
    'order_purchase_timestamp', 'order_approved_at',
    'order_delivered_carrier_date', 'order_delivered_customer_date',
    'order_estimated_delivery_date', 'shipping_limit_date'
]

def convert_to_datetime_utc(df):
    """Converte colunas temporais para UTC."""
    df_conv = df.copy()
    for col in DATE_COLUMNS:
        if col in df_conv.columns:
            df_conv[col] = pd.to_datetime(df_conv[col], errors='coerce', utc=True)
    return df_conv

def validate_pedidos(df):
    """Regras específicas para PEDIDOS"""
    df_dq = df.copy()
    print("   🛡️ Validando regras de PEDIDOS...")
   
    # Regra: Entrega antes da compra
    if 'order_delivered_customer_date' in df_dq.columns and 'order_purchase_timestamp' in df_dq.columns:
        df_dq['dq_erro_cronologia'] = df_dq['order_delivered_customer_date'] < df_dq['order_purchase_timestamp']
   
    # Regra: Data no futuro
    if 'order_purchase_timestamp' in df_dq.columns:
        now_utc = pd.Timestamp.now(tz='UTC')
        df_dq['dq_erro_futuro'] = df_dq['order_purchase_timestamp'] > now_utc
        
    return df_dq

def validate_itens(df):
    """Regras específicas para ITENS"""
    df_dq = df.copy()
    print("   🛡️ Validando regras de ITENS...")

    # Regra: Data limite de envio nula ou inválida
    if 'shipping_limit_date' in df_dq.columns:
        df_dq['dq_erro_data_limite'] = df_dq['shipping_limit_date'].isna()
        
    return df_dq