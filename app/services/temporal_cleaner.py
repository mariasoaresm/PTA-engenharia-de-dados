import pandas as pd
import matplotlib.pyplot as plt
import os

class TemporalCleaner:
    """
    Classe responsável pela padronização temporal e validação de regras de negócio.
    """
    
    def __init__(self):
        # Define colunas de data que esperamos encontrar
        self.date_columns = [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date',
            'shipping_limit_date'
        ]

    def convert_to_datetime_utc(self, df):
        """
        Converte colunas definidas para datetime64[ns, UTC].
        """
        df_conv = df.copy()
        print("   [TemporalCleaner] Iniciando conversão de tipos...")
        
        for col in self.date_columns:
            if col in df_conv.columns:
                # errors='coerce' transforma erros em NaT (nulos de data)
                df_conv[col] = pd.to_datetime(df_conv[col], errors='coerce', utc=True)
        
        return df_conv

    def validate_business_rules(self, df):
        """
        Aplica validações lógicas (Data Quality) e cria flags de erro.
        """
        df_dq = df.copy()
        print("   [TemporalCleaner] Validando regras de negócio...")

        # 1. Regra: Entrega antes da Compra
        if 'order_delivered_customer_date' in df_dq.columns and 'order_purchase_timestamp' in df_dq.columns:
            df_dq['dq_erro_cronologia'] = df_dq['order_delivered_customer_date'] < df_dq['order_purchase_timestamp']

        # 2. Regra: Compra no Futuro
        if 'order_purchase_timestamp' in df_dq.columns:
            now_utc = pd.Timestamp.now(tz='UTC')
            df_dq['dq_erro_futuro'] = df_dq['order_purchase_timestamp'] > now_utc

        return df_dq

    def generate_visual_validation(self, df, output_path='data/refined/plots'):
        """
        Gera gráfico de validação e salva em disco.
        """
        if 'order_purchase_timestamp' not in df.columns:
            return

        # Cria pasta se não existir
        os.makedirs(output_path, exist_ok=True)
        
        plt.figure(figsize=(10, 5))
        valid_dates = df['order_purchase_timestamp'].dropna()
        
        plt.hist(valid_dates, bins=20, color='#4CAF50', edgecolor='black')
        plt.title('Distribuição Temporal das Compras')
        plt.xlabel('Data')
        plt.ylabel('Quantidade')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        filename = os.path.join(output_path, 'validacao_temporal.png')
        plt.savefig(filename)
        plt.close() # Fecha para libertar memória
        print(f"   [TemporalCleaner] Gráfico salvo em: {filename}")