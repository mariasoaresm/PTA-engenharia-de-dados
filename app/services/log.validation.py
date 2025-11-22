import logging
import uuid
import time
import pandas as pd
import numpy as np

# configuração básica de logging e função orfã
def setup_logging(log_level=logging.INFO):
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    return logging.getLogger(__name__)

logger = setup_logging()

def get_orphan_mask(df_order_items, df_orders, df_products, df_sellers):
    valid_order_ids = df_orders['order_id'].unique()
    valid_product_ids = df_products['product_id'].unique()
    valid_seller_ids = df_sellers['seller_id'].unique()
    
    mask_orphan_order = ~df_order_items['order_id'].isin(valid_order_ids)
    mask_orphan_product = ~df_order_items['product_id'].isin(valid_product_ids)
    mask_orphan_seller = ~df_order_items['seller_id'].isin(valid_seller_ids)
    
    return mask_orphan_order | mask_orphan_product | mask_orphan_seller


def process_payload_with_logging(payload, max_rows_limit=10000, request_user="API_Caller"):

    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    audit_metrics = {
        'request_id': request_id,
        'request_user': request_user,
        'processing_start_timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_time)),
        'total_processing_time_sec': 0.0,
        'payload_sizes': {},
        'orphan_count': 0,
        'discarded_count': 0,
        'validations_passed': True
    }

    logger.info(f"[{request_id}] Processamento iniciado. Usuário: {request_user}")

    try:
        total_rows = sum(len(df) for df in payload.values())
        if total_rows > max_rows_limit:
            logger.error(f"[{request_id}] FALHA: Payload excede o limite de linhas ({total_rows} > {max_rows_limit}).")
            raise ValueError("Payload muito grande.")
            
        for name, df in payload.items():
            rows = len(df)
            audit_metrics['payload_sizes'][name] = rows
            logger.info(f"[{request_id}] Recebido dataset '{name}' com {rows} linhas.")
            
    except Exception as e:
        logger.error(f"[{request_id}] Erro fatal na validação de tamanho: {e}", exc_info=True)
        return None

    processed_payload = {}
    
    # validações e conversões por dataset
    for dataset_name, df in payload.items():
        processing_start = time.time()
        logger.info(f"[{request_id}] Iniciando validação para '{dataset_name}'.")

        # verificação de colunas essenciais
        required_cols = {
            'orders': ['order_id', 'order_purchase_timestamp'],
            'products': ['product_id'],
            'sellers': ['seller_id'],
            'order_items': ['order_id', 'product_id', 'seller_id', 'price', 'freight_value', 'shipping_limit_date']
        }.get(dataset_name, [])

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"[{request_id}] WARNING: Colunas essenciais faltando em '{dataset_name}': {missing_cols}")
            audit_metrics['validations_passed'] = False
            
        # cópia do dataframe para evitar alterações no original
        df_copy = df.copy() 
        
        # conversão de datas
        date_cols = [col for col in df_copy.columns if 'date' in col or 'timestamp' in col]
        for col in date_cols:
            if col in df_copy.columns:
                initial_na_count = df_copy[col].isna().sum()
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
                new_na_count = df_copy[col].isna().sum()
                if new_na_count > initial_na_count:
                    warnings_count = new_na_count - initial_na_count
                    logger.warning(f"[{request_id}] WARNING: {warnings_count} datas inválidas/inconsistentes (NaT) em '{dataset_name}.{col}'.")
                    audit_metrics['validations_passed'] = False # ✅ CORREÇÃO: Data inválida = Falha na Validação
        
        # conversão de numéricos 
        if dataset_name == 'order_items':
            for col in ['price', 'freight_value']:
                if col in df_copy.columns:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                    na_after_coerce = df_copy[col].isna().sum() - df[col].isna().sum()
                    if na_after_coerce > 0:
                         logger.warning(f"[{request_id}] WARNING: {na_after_coerce} valores não numéricos em '{col}' (convertidos para NaN).")
                         audit_metrics['validations_passed'] = False
                    
        processed_payload[dataset_name] = df_copy
        
        processing_end = time.time()
        time_elapsed = processing_end - processing_start
        logger.info(f"[{request_id}] Validação de '{dataset_name}' concluída em {time_elapsed:.4f}s.")
        
    # tratamento de registros órfãos
    if all(name in processed_payload for name in ['order_items', 'orders', 'products', 'sellers']):
        df_items = processed_payload['order_items']
        
        # contagem e tratamento de órfãos
        mask_is_orphan = get_orphan_mask(
            df_items, 
            processed_payload['orders'], 
            processed_payload['products'], 
            processed_payload['sellers']
        )
        
        orphan_count = mask_is_orphan.sum()
        audit_metrics['orphan_count'] = orphan_count
        
        if orphan_count > 0:
            logger.warning(f"[{request_id}] {orphan_count} registros órfãos identificados em 'order_items'.")
            audit_metrics['validations_passed'] = False
            
            df_items_valid = df_items[~mask_is_orphan].copy()
            audit_metrics['discarded_count'] = orphan_count
            processed_payload['order_items'] = df_items_valid
            logger.info(f"[{request_id}] {orphan_count} registros órfãos removidos de 'order_items'.")

    # fim do processamento e log de métricas finais
    end_time = time.time()
    audit_metrics['total_processing_time_sec'] = end_time - start_time
    
    logger.info(f"[{request_id}] Processamento CONCLUÍDO. Tempo Total: {audit_metrics['total_processing_time_sec']:.4f}s")
    logger.info(f"[{request_id}] Métricas de auditoria: {audit_metrics}")

    return {"error": "Payload inválido", "request_id": request_id}