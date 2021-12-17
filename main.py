from conexao import ListaNotas, Conexao
import pandas as pd
import numpy as np
from dataxml import XmlNota
from datetime import datetime

conn = Conexao('cosmos', 'dbnfe')

COLUNAS = ['cProd', 'cEAN', 'xProd', 'CFOP', 'uCom', 'qCom', 'vUnCom',
           'vDesc', 'vProd',
           'nLote', 'qLote', 'dFab', 'dVal', 'vBC', 'pICMS',
           'vICMS', 'pPIS', 'vPIS', 'pCOFINS', 'vCOFINS']

COL_TIPO = {'qCom': np.float64, 'vUnCom': np.float64,
            'vDesc': np.float64, 'vProd': np.float64,
            'qLote': np.float64, 'dFab': np.datetime64,
            'dVal': np.datetime64, 'ICMS_vBC': np.float64,
            'pICMS': np.float64,
            'PIS_vBC': np.float64, 'COFINS_vBC': np.float64,
            'vICMS': np.float64, 'pPIS': np.float64,
            'vPIS': np.float64, 'pCOFINS': np.float64,
            'vCOFINS': np.float64,
            'vNF': np.float64, 'nNF': np.int64,
            'vICMS_nota': np.float64,
            'Serie': np.int64, 'cnpjEmi': np.int64,
            'dhEmi': np.datetime64, 'Id': np.int64}

c = ('ESTORNO-INCINERACAO', )
notas = ListaNotas.controle(
    conn.conectar(), *c, inicio=datetime(2021, 11, 1), fim=datetime(2021, 11, 15))

lista_df = []
for df in notas:
    try:
        dados = XmlNota(df, COLUNAS)
        df = pd.DataFrame(list(dados.dados()))
        lista_df.append(df)
    except Exception as e:
        print(df.chave, e)

        next

df_comp = pd.concat(lista_df, ignore_index=True)
conversoes = {k: COL_TIPO[k] for k in df_comp.keys() if k in COL_TIPO.keys()}
df_comp = df_comp.astype(conversoes)


# saida do arquivo para o excel
df_comp.to_excel('Dados.xlsx', index=False)
