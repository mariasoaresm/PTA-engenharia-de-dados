import pandas as pd
from typing import Dict, List, Any
from services.cleaners import DataCleaner
from services.validators import IntegrityValidator

class ETLProcessor:
    def __init__(self):
        self.cleaner_map = {
            'orders': DataCleaner.clean_orders,
            'products': DataCleaner.clean_products,
            'order_items': DataCleaner.clean_items,
            'vendedores': DataCleaner.clean_generic
        }

    def process_payload(self, payload: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        processed_dfs = {}
        orphans_dfs = {}
        
        # 1. Limpeza
        for entity_name, raw_data in payload.items():
            if not raw_data: continue
            
            df = pd.DataFrame(raw_data)
            cleaner_func = self.cleaner_map.get(entity_name, DataCleaner.clean_generic)
            
            try:
                df_clean = cleaner_func(df)
                processed_dfs[entity_name] = df_clean
            except Exception as e:
                print(f"Erro ao processar {entity_name}: {e}")
                continue

        # 2. Validação (Items -> Orders)
        if 'order_items' in processed_dfs:
            items_df = processed_dfs['order_items']
            orders_df = processed_dfs.get('orders', pd.DataFrame())
            
            valid_items, orphan_items = IntegrityValidator.validate_referential_integrity(
                child_df=items_df, parent_df=orders_df, child_key='order_id', parent_key='order_id'
            )
            
            processed_dfs['order_items'] = valid_items
            if not orphan_items.empty:
                orphans_dfs['order_items'] = orphan_items

        # 3. Formatação Final
        final_response = {"status": "success", "data": {}, "orphans": {}}

        for entity, df in processed_dfs.items():
            final_response["data"][entity] = df.to_dict(orient="records")

        for entity, df in orphans_dfs.items():
            final_response["orphans"][entity] = df.to_dict(orient="records")

        return final_response

etl_processor = ETLProcessor()