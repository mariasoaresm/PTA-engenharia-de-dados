# Arquivos Legados (Depreciados)

## Contexto
Estes scripts representam a versão antiga do pipeline de ETL (v1.0), que operava lendo arquivos `.csv` diretamente do disco.
Eles foram descontinuados em favor da nova arquitetura orientada a serviços (API/FastAPI) para permitir integração em tempo real e maior escalabilidade.

## Conteúdo
- **run_etl.py**: Antigo orquestrador baseado em glob/arquivos.
- **data_cleaner.py**: Lógicas antigas de limpeza (substituídas por `services/cleaners.py`).
- **temporal_cleaner.py**: Lógicas antigas de data (substituídas por `services/cleaners.py`).

**Atenção:** Não utilize estes arquivos em produção. Mantidos apenas para histórico.