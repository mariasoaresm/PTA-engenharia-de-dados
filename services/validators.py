import pandas as pd
from typing import Tuple, Dict, Any

class IntegrityValidator:
    """Responsável por validar a integridade referencial entre datasets."""

    @staticmethod
    def validate_referential_integrity(
        child_df: pd.DataFrame, 
        parent_df: pd.DataFrame, 
        child_key: str, 
        parent_key: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        
        if parent_df is None or parent_df.empty:
            return pd.DataFrame(columns=child_df.columns), child_df

        if child_df.empty:
            return child_df, pd.DataFrame(columns=child_df.columns)

        if child_key not in child_df.columns:
            raise ValueError(f"Chave {child_key} não encontrada no dataset filho.")
        
        if parent_key not in parent_df.columns:
            raise ValueError(f"Chave {parent_key} não encontrada no dataset pai.")

        valid_ids = set(parent_df[parent_key].unique())
        mask_valid = child_df[child_key].isin(valid_ids)
        
        valid_records = child_df[mask_valid].copy()
        orphan_records = child_df[~mask_valid].copy()
        
        if not orphan_records.empty:
            orphan_records['dq_issue'] = f"Orphan: {child_key} not found in parent dataset"

        return valid_records, orphan_records