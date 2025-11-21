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

    def __init__(self):
        # --- ATUALIZAÇÃO PARA FUNCIONAR NO VS CODE ---
        # Define o diretório base (pasta raiz do projeto)
        # Isso garante que funcione independente de onde você rodar
        base_dir = os.getcwd()
        
        # Se estiver rodando de dentro da pasta 'app', sobe um nível
        if os.path.basename(base_dir) == 'app':
            base_dir = os.path.dirname(base_dir)

        # Define onde estão os arquivos originais (RAW) e onde salvar (PROCESSED)
        # DICA: Crie uma pasta 'data/raw' e coloque seus arquivos originais lá!
        self.input_path = os.path.join(base_dir, 'data', 'raw') + os.sep
        self.output_path = os.path.join(base_dir, 'data', 'processed') + os.sep
        
        # Cria as pastas se não existirem
        os.makedirs(self.input_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        
        print(f"📂 Input: {self.input_path}")
        print(f"📂 Output: {self.output_path}")

    def _load_file(self, suffix: str) -> pd.DataFrame:
        """
        Busca arquivos dinamicamente usando sufixo para ignorar nomes complexos.
        Ex: Procura por '*pedidos.csv' e acha '[Duda] DataLake - pedidos.csv'.
        """
        # O glob busca na pasta definida no init
        files = glob.glob(f"{self.input_path}*{suffix}")
        if not files:
            print(f"⚠️  AVISO: Arquivo finalizado em '{suffix}' não encontrado em 'data/raw'.")
            return None # Retorna None para tratar depois
        
        file_path = files[0]
        print(f"📖 Lendo: {os.path.basename(file_path)}")
        
        try:
            return pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding='latin-1')

    def _save_file(self, df: pd.DataFrame, filename: str):
        """Padroniza o salvamento na pasta processed."""
        if df is None:
            return
            
        path = f"{self.output_path}{filename}"
        df.to_csv(path, index=False)
        print(f"✅ Salvo em: {path}")

    def process_orders(self):
        """Regra de Negócio: Pedidos."""
        df = self._load_file("pedidos.csv")
        if df is None: return

        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                     'order_delivered_carrier_date', 'order_delivered_customer_date', 
                     'order_estimated_delivery_date']
        
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        self._save_file(df, "pedidos_clean.csv")

    def process_products(self):
        """Regra de Negócio: Produtos."""
        df = self._load_file("produtos.csv")
        if df is None: return

        if 'product_category_name' in df.columns:
            df['product_category_name'] = df['product_category_name'].fillna('outros')

        cols_zero = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
        for col in cols_zero:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        cols_dims = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        for col in cols_dims:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())

        self._save_file(df, "produtos_clean.csv")

    def process_others(self):
        """Regra de Negócio: Itens e Vendedores."""
        # Note que ajustei para procurar 'itens' e não 'itens_pedidos.csv' exato
        # para ser mais flexível com nomes como 'clean_itens_pedidos.csv'
        targets = [("itens", "clean_itens_pedidos.csv"), ("vendedores", "clean_vendedores.csv")]
        
        for suffix, output_name in targets:
            try:
                df = self._load_file(f"{suffix}.csv") # Busca qualquer coisa que termine com itens.csv
                if df is not None:
                    self._save_file(df, output_name)
            except Exception as e:
                print(f"⚠️ Erro ao processar {suffix}: {e}")

    def run(self):
        """Método público que orquestra todo o pipeline."""
        print("--- 🚀 Iniciando Pipeline de Limpeza (Raw -> Processed) ---")
        self.process_orders()
        self.process_products()
        self.process_others()
        print("--- ✨ Pipeline Finalizado ---")

if __name__ == "__main__":
    pipeline = DataCleaningPipeline()
    pipeline.run()