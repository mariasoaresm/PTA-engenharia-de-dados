import os
import pandas as pd
import glob

# Importa a classe que acabámos de criar
# O 'app' é o nome da pasta raiz do pacote
from services.temporal_cleaner import TemporalCleaner

def main():
    # 1. Configuração de Caminhos
    # Ajusta estes caminhos conforme onde a tua pasta 'data' estiver
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Sobe um nível para a raiz
    INPUT_DIR = os.path.join(BASE_DIR, 'data', 'processed')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'refined')
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 2. Instancia o Serviço
    cleaner = TemporalCleaner()

    # 3. Busca ficheiros
    csv_files = glob.glob(os.path.join(INPUT_DIR, "*clean*.csv"))
    
    if not csv_files:
        print(f"⚠️ Nenhum ficheiro encontrado em {INPUT_DIR}")
        return

    for file_path in csv_files:
        filename = os.path.basename(file_path)
        print(f"\n📂 Processando: {filename}")

        # Carrega
        df = pd.read_csv(file_path)

        # Aplica Conversão
        df_converted = cleaner.convert_to_datetime_utc(df)

        # Aplica Regras (apenas se for o dataset principal de pedidos)
        if 'pedidos' in filename:
            df_final = cleaner.validate_business_rules(df_converted)
            cleaner.generate_visual_validation(df_final, output_path=os.path.join(OUTPUT_DIR, 'plots'))
        else:
            df_final = df_converted

        # Salva
        output_path = os.path.join(OUTPUT_DIR, f"refined_{filename}")
        df_final.to_csv(output_path, index=False)
        print(f"✅ Salvo em: {output_path}")

if __name__ == "__main__":
    main()