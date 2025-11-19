def tratar_registros_orfaos(df_order_items, df_orders, df_products, df_sellers, acao='marcar'):

    # 1. Obter IDs únicos das tabelas DIMENSÃO para validação (Chaves Primárias)
    valid_order_ids = df_orders['order_id'].unique()
    valid_product_ids = df_products['product_id'].unique()
    valid_seller_ids = df_sellers['seller_id'].unique()

    # 2. Criar máscaras booleanas para cada verificação:

    mask_orphan_order = ~df_order_items['order_id'].isin(valid_order_ids)

    mask_orphan_product = ~df_order_items['product_id'].isin(valid_product_ids)

    mask_orphan_seller = ~df_order_items['seller_id'].isin(valid_seller_ids)

    # 3. Combinar as máscaras: um registro é órfão se falhar EM QUALQUER UMA das validações
    mask_is_orphan = mask_orphan_order | mask_orphan_product | mask_orphan_seller

    # 4. Aplicar a ação
    if acao == 'marcar':
        # Adiciona a coluna 'is_orphan' (Booleana)
        df_order_items['is_orphan'] = mask_is_orphan
        return df_order_items
    
    elif acao == 'remover':
        # Filtra o DataFrame para retornar apenas os registros válidos (NÃO órfãos)
        df_validos = df_order_items[~mask_is_orphan].copy()
        return df_validos
    
    else:
        raise ValueError("Ação inválida. Use 'marcar' ou 'remover'.")