import pandas as pd
import numpy as np

# ==========================================================
# CLEANERS — Funções soltas (agora compatíveis com o ETLProcessor)
# ==========================================================

def limpar_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()

    date_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]

    for col in date_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

    return df_clean


def limpar_produtos(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()

    if 'product_category_name' in df_clean.columns:
        df_clean['product_category_name'] = df_clean['product_category_name'].fillna('outros')

    cols_zero = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    for col in cols_zero:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna(0)

    cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in cols_dims:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna(df_clean[col].median())

    return df_clean


def limpar_itens(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


def limpar_vendedores(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


# Função opcional caso você precise sanitizar para JSON
def sanitize_df(df: pd.DataFrame):
    df = df.replace({np.nan: None})
    return df.to_dict(orient="records")
