from conexao import Conexao
import pandas as pd
from dataxml import XmlNota, ListaNotas

conn = Conexao('servidor', 'tabela')

c = ('CHAVE DE ACESSO', )
notas = ListaNotas.chave(conn.conectar(), *c)

lista_df = []
for df in notas:
    try:
        dados = XmlNota(df)
        df = pd.DataFrame(list(dados.dados()))
        lista_df.append(df)
    except Exception as e:
        print(df.chave, e)

        next

df_comp = pd.concat(lista_df, ignore_index=True)

# saida do arquivo para o excel
df_comp.to_excel('Dados.xlsx', index=False)
