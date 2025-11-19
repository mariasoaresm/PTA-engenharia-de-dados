import pandas as pd
import numpy as np
import os
import glob

class DataCleaningPipeline:
    """
    Pipeline de ETL responsável pela ingestão, limpeza e padronização dos dados.
    Estrutura:
    1. Identifica arquivos ignorando prefixos (ex: [Duda]).
    2. Aplica regras de negócio (datas, nulos, imputação).
    3. Salva dados prontos na camada 'processed'.
    """

    def __init__(self, input_path: str = "/content/", output_path: str = "/content/data/processed/"):
        # Define caminhos de origem (Raw) e destino (Trusted/Processed)
        self.input_path = input_path
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

    def _load_file(self, suffix: str) -> pd.DataFrame:
        """
        Busca arquivos dinamicamente usando sufixo para ignorar nomes complexos.
        Ex: Procura por '*pedidos.csv' e acha '[Duda] DataLake - pedidos.csv'.
        """
        files = glob.glob(f"{self.input_path}*{suffix}")
        if not files:
            raise FileNotFoundError(f"Arquivo finalizado em {suffix} não encontrado.")
        
        file_path = files[0]
        print(f"📂 Lendo: {os.path.basename(file_path)}")
        
        # Tenta carregar com UTF-8, fallback para Latin-1 (comum no Brasil)
        try:
            return pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding='latin-1')

    def _save_file(self, df: pd.DataFrame, filename: str):
        """Padroniza o salvamento na pasta processed."""
        path = f"{self.output_path}{filename}"
        df.to_csv(path, index=False)
        print(f"✅ Salvo em: {path}")

    def process_orders(self):
        """
        Regra de Negócio: Pedidos.
        - Conversão de strings para datetime.
        - Mantém NaT (Not a Time) pois indica que o evento (ex: entrega) ainda não ocorreu.
        """
        df = self._load_file("pedidos.csv")
        
        # Lista de colunas de data para conversão
        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                     'order_delivered_carrier_date', 'order_delivered_customer_date', 
                     'order_estimated_delivery_date']
        
        # 'coerce' transforma erros/nulos em NaT automaticamente
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        self._save_file(df, "pedidos_clean.csv")

    def process_products(self):
        """
        Regra de Negócio: Produtos.
        - Categoricos nulos -> 'outros'.
        - Contagens nulas -> 0.
        - Dimensões/Pesos nulos -> Mediana (robusto a outliers).
        """
        df = self._load_file("produtos.csv")

        # 1. Tratamento Categórico
        df['product_category_name'] = df['product_category_name'].fillna('outros')

        # 2. Tratamento Numérico (Contagens)
        cols_zero = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
        df[cols_zero] = df[cols_zero].fillna(0)

        # 3. Tratamento Numérico (Dimensões Físicas)
        cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        for col in cols_dims:
            df[col] = df[col].fillna(df[col].median())

        self._save_file(df, "produtos_clean.csv")

    def process_others(self):
        """
        Regra de Negócio: Itens e Vendedores.
        - Move dados já limpos para a zona processed para manter consistência do Data Lake.
        """
        for file in ["itens_pedidos.csv", "vendedores.csv"]:
            try:
                df = self._load_file(file)
                # Adiciona prefixo clean_ para padronizar saída
                self._save_file(df, f"clean_{file}")
            except Exception as e:
                print(f"⚠️ Erro ao processar {file}: {e}")

    def run(self):
        """Método público que orquestra todo o pipeline."""
        print("--- 🚀 Iniciando Pipeline de ETL ---")
        self.process_orders()
        self.process_products()
        self.process_others()
        print("--- ✨ Pipeline Finalizado com Sucesso ---")

# --- Ponto de Entrada (Execution) ---
if __name__ == "__main__":
    # Instancia e executa
    pipeline = DataCleaningPipeline()
    pipeline.run()