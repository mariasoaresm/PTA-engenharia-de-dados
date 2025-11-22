import pandas as pd
import numpy as np

def tratar_outliers_iqr(df, coluna, fator_iqr=1.5, metodo='capping'):

    # REGRA DE SEGURANÇA: Retorna DF original se for muito pequeno
    if len(df) < 3:
        print(f"AVISO: DataFrame muito pequeno para coluna {coluna}")
        return df

    # 1. Calcular Q1, Q3 e IQR (necessita de Pandas para .quantile())
    Q1 = df[coluna].quantile(0.25)
    Q3 = df[coluna].quantile(0.75)
    IQR = Q3 - Q1

    # 2. Calcular os limites
    limite_inferior = Q1 - fator_iqr * IQR
    limite_superior = Q3 + fator_iqr * IQR

    if metodo == 'capping':

        if pd.isna(limite_inferior) or pd.isna(limite_superior):
            print(f"AVISO: Limites IQR NaN/Inf para coluna '{coluna}'. Retornando sem tratamento de Outliers")
            return df

        df[coluna] = np.where(df[coluna] < limite_inferior, limite_inferior, df[coluna])
        df[coluna] = np.where(df[coluna] > limite_superior, limite_superior, df[coluna])
        
        return df
    
    elif metodo == 'remover':
        # Remove as linhas que contêm outliers
        df_tratado = df[
            (df[coluna] >= limite_inferior) &
            (df[coluna] <= limite_superior)
        ].copy()
        return df_tratado
    else:
        raise ValueError("Método inválido. Use 'capping' ou 'remover'.")