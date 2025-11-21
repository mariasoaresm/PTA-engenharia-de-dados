import pandas as pd
import numpy as np
from typing import List

class DataCleaner:
    """
    Módulo contendo funções puras de limpeza de dados.
    Aplica regras de negócio, tipagem e normalização de esquema.
    """

    @staticmethod
    def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Remove espaços e converte colunas para minúsculas."""
        df = df.copy()
        df.columns = df.columns.str.strip().str.lower()
        return df

    @staticmethod
    def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
        """Processa dados de pedidos: conversão de datas UTC e validações básicas."""
        df = DataCleaner._standardize_columns(df)
        
        date_cols = [
            'order_purchase_timestamp', 'order_approved_at', 
            'order_delivered_carrier_date', 'order_delivered_customer_date', 
            'order_estimated_delivery_date'
        ]

        # Conversão Vetorizada
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

        # Regra: Remover pedidos sem timestamp de compra
        if 'order_purchase_timestamp' in df.columns:
            df = df.dropna(subset=['order_purchase_timestamp'])

        return df

    @staticmethod
    def clean_products(df: pd.DataFrame) -> pd.DataFrame:
        """Processa dados de produtos: inputação de nulos e formatação."""
        df = DataCleaner._standardize_columns(df)

        if 'product_category_name' in df.columns:
            df['product_category_name'] = df['product_category_name'].fillna('outros')

        cols_zero = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
        for col in cols_zero:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        for col in cols_dims:
            if col in df.columns:
                median_val = df[col].median() if not df[col].isnull().all() else 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(median_val)

        return df

    @staticmethod
    def clean_items(df: pd.DataFrame) -> pd.DataFrame:
        """Processa itens de pedido."""
        df = DataCleaner._standardize_columns(df)
        if 'shipping_limit_date' in df.columns:
             df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce', utc=True)
        return df

    @staticmethod
    def clean_generic(df: pd.DataFrame) -> pd.DataFrame:
        return DataCleaner._standardize_columns(df)