# app/services/processor_core.py
import logging
import pandas as pd
from typing import Dict, List, Any
from app.services import data_cleaner  # importa o módulo todo
from app.services.validators import IntegrityValidator

logger = logging.getLogger("pta-etl-api.processor")

# map com funções exportadas do módulo data_cleaner
CLEANER_MAP = {
    "orders": data_cleaner.limpar_pedidos,
    "products": data_cleaner.limpar_produtos,
    "order_items": data_cleaner.limpar_itens,
    "sellers": data_cleaner.limpar_vendedores,
}

class ETLProcessor:
    def __init__(self):
        self.cleaner_map = CLEANER_MAP

    def process_payload(self, payload: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        processed_dfs = {}
        orphans_dfs = {}

        for entity_name, raw_data in payload.items():
            try:
                if not raw_data:
                    continue
                df = pd.DataFrame(raw_data)
                cleaner_func = self.cleaner_map.get(entity_name)
                if cleaner_func is None:
                    logger.warning("Nenhum cleaner para entidade: %s", entity_name)
                    continue
                df_clean = cleaner_func(df)
                processed_dfs[entity_name] = df_clean
            except Exception as e:
                logger.exception("Erro ao limpar entidade %s: %s", entity_name, e)
                raise  # re-raise para ser capturado no main e mostrar stacktrace

        # validação de integridade (itens -> orders)
        try:
            if "order_items" in processed_dfs:
                items_df = processed_dfs["order_items"]
                orders_df = processed_dfs.get("orders", pd.DataFrame())
                valid_items, orphan_items = IntegrityValidator.validate_referential_integrity(
                    child_df=items_df, parent_df=orders_df, child_key="order_id", parent_key="order_id"
                )
                processed_dfs["order_items"] = valid_items
                if not orphan_items.empty:
                    orphans_dfs["order_items"] = orphan_items
        except Exception as e:
            logger.exception("Erro na validação de integridade: %s", e)
            raise

        # formata resposta final (converte dataframes pra dict)
        final_response = {"status": "success", "data": {}, "orphans": {}}
        for entity, df in processed_dfs.items():
            try:
                final_response["data"][entity] = df.to_dict(orient="records")
            except Exception as e:
                logger.exception("Erro ao converter df para records na entidade %s: %s", entity, e)
                raise

        for entity, df in orphans_dfs.items():
            try:
                final_response["orphans"][entity] = df.to_dict(orient="records")
            except Exception as e:
                logger.exception("Erro ao converter orphans df para records na entidade %s: %s", entity, e)
                raise

        return final_response

# instância global
etl_processor = ETLProcessor()
