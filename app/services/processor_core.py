import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from app.services import data_cleaner  # Módulo com as funções de limpeza
from app.services.validators import IntegrityValidator # Assumindo que esta classe existe

logger = logging.getLogger("pta-etl-api.processor")

# Mapeamento das funções de limpeza (usa 'items' e 'sellers' para compatibilidade)
CLEANER_MAP = {
    "orders": data_cleaner.limpar_pedidos,
    "products": data_cleaner.limpar_produtos,
    "items": data_cleaner.limpar_itens, # Corrigido para 'items' (nome interno do payload)
    "sellers": data_cleaner.limpar_vendedores,
}

class ETLProcessor:
    def __init__(self):
        self.cleaner_map = CLEANER_MAP

    def process_payload(self, payload: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        processed_dfs = {}
        orphans_dfs = {}

        # 1. Limpeza e Transformação para cada entidade
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
                raise

        # 2. Validação de Integridade Referencial (CRÍTICA: order_items/items)
        try:
            # Usamos 'items' pois é o nome interno do Payload corrigido
            if "items" in processed_dfs:
                items_df = processed_dfs["items"]
                
                # --- ATENÇÃO: TRATAMENTO DE ÓRFÃOS PARA AS 3 CHAVES É SEQUENCIAL ---
                
                # 1. order_items -> orders
                items_df, orphan_orders = IntegrityValidator.validate_referential_integrity(
                    child_df=items_df, parent_df=processed_dfs.get("orders", pd.DataFrame()), child_key="order_id", parent_key="order_id"
                )
                
                # 2. order_items -> products (Seu órfão de teste está aqui!)
                items_df, orphan_products = IntegrityValidator.validate_referential_integrity(
                    child_df=items_df, parent_df=processed_dfs.get("products", pd.DataFrame()), child_key="product_id", parent_key="product_id"
                )
                
                # 3. order_items -> sellers
                items_df, orphan_sellers = IntegrityValidator.validate_referential_integrity(
                    child_df=items_df, parent_df=processed_dfs.get("sellers", pd.DataFrame()), child_key="seller_id", parent_key="seller_id"
                )

                processed_dfs["items"] = items_df
                
                # Adiciona órfãos para logs
                if not orphan_orders.empty: orphans_dfs["items_orders"] = orphan_orders
                if not orphan_products.empty: orphans_dfs["items_products"] = orphan_products
                if not orphan_sellers.empty: orphans_dfs["items_sellers"] = orphan_sellers
                    
        except Exception as e:
            logger.exception("Erro na validação de integridade: %s", e)
            raise

        # 3. Formata resposta final e Sanitização de Saída (NaN/NaT -> None)
        final_response = {"status": "success", "data": {}, "orphans": {}}
        
        for entity, df in processed_dfs.items():
            try:
                # CORREÇÃO CRÍTICA: Substitui np.nan e NaT por None (JSON null) antes de serializar
                df_sanitized = df.replace({np.nan: None})
                
                # Para fins de retorno, o nome 'items' deve ser mapeado de volta para 'order_items' 
                # na chave de saída, se necessário. Aqui assumimos que o Pydantic OutputPayload
                # aceitará a chave mapeada ou renomeamos antes de retornar:
                key_name = 'order_items' if entity == 'items' else entity
                
                final_response["data"][key_name] = df_sanitized.to_dict(orient="records")
            except Exception as e:
                logger.exception("Erro ao converter df para records na entidade %s: %s", entity, e)
                raise

        # Sanitiza e adiciona os órfãos
        for entity, df in orphans_dfs.items():
             try:
                df_sanitized = df.replace({np.nan: None})
                final_response["orphans"][entity] = df_sanitized.to_dict(orient="records")
             except Exception as e:
                logger.exception("Erro ao converter orphans df para records na entidade %s: %s", entity, e)
                raise

        return final_response

etl_processor = ETLProcessor()