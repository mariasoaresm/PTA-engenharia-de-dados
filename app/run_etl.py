import os
import glob
import shutil
import pandas as pd

# --- IMPORTAÇÃO DAS FUNÇÕES QUE CRIAMOS NO OUTRO ARQUIVO ---
# O 'sys' e o 'append' abaixo ajudam o Python a encontrar a pasta services se necessário
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from services.temporal_cleaner import convert_to_datetime_utc, validate_pedidos, validate_itens
except ImportError:
    # Caso esteja rodando da raiz do projeto
    from app.services.temporal_cleaner import convert_to_datetime_utc, validate_pedidos, validate_itens

# ==============================================================================
# PARTE 1: CONFIGURAÇÃO DE PASTAS (Baseada na Célula 1 do Colab)
# ==============================================================================
print("🔄 INICIANDO CONFIGURAÇÃO DE AMBIENTE LOCAL...")

# Define o diretório base como o local onde o projeto está
# Assumindo que você roda o script da raiz do projeto ou da pasta app
BASE_DIR = os.getcwd() 

# Ajuste para garantir que caia na pasta 'data' correta independente de onde rodar
if os.path.basename(BASE_DIR) == 'app':
    BASE_DIR = os.path.dirname(BASE_DIR) # Sobe um nível para a raiz

RAW_DIR = os.path.join(BASE_DIR, 'data', 'processed')
REFINED_DIR = os.path.join(BASE_DIR, 'data', 'refined')

# Criar as pastas (se não existirem)
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(REFINED_DIR, exist_ok=True)
print(f"✅ Pastas configuradas: \n - {RAW_DIR} \n - {REFINED_DIR}")

# ==============================================================================
# PARTE 2: EXECUÇÃO DO PIPELINE (Baseada na Célula 3 do Colab)
# ==============================================================================

# Pega todos os CSVs na pasta processed
files = glob.glob(os.path.join(RAW_DIR, '*.csv'))

print(f"\n🚀 INICIANDO PIPELINE PARA {len(files)} ARQUIVOS ENCONTRADOS EM 'data/processed'...\n")

if not files:
    print("⚠️  AVISO: Nenhum arquivo .csv encontrado na pasta 'data/processed'.")
    print("👉 Certifique-se de colocar seus arquivos csv dentro de PTA-ENGENHARIA-DE-DADOS/data/processed")

for file_path in files:
    filename = os.path.basename(file_path)
    print(f"📄 PROCESSANDO: {filename}")
   
    # 1. Carregar
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Erro ao ler {filename}: {e}")
        continue

    # 2. Converter Datas (Usa a função importada de temporal_cleaner)
    df = convert_to_datetime_utc(df)

    # 3. Decisão (Branching Logic)
    if 'itens' in filename.lower() or 'items' in filename.lower():
        df_final = validate_itens(df)
        tipo = "ITENS"
    elif 'pedidos' in filename.lower() or 'orders' in filename.lower():
        df_final = validate_pedidos(df)
        tipo = "PEDIDOS"
    else:
        df_final = df
        tipo = "GENÉRICO"
        print("   ℹ️ Nenhum validador específico encontrado. Apenas conversão de data aplicada.")

    # 4. Salvar
    output_name = f"refined_{filename}"
    save_path = os.path.join(REFINED_DIR, output_name)
    df_final.to_csv(save_path, index=False)
    print(f"   💾 [{tipo}] Salvo com sucesso em: {save_path}\n")

print("🏁 PIPELINE CONCLUÍDO.")