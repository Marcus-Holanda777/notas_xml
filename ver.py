# import xml.etree.ElementTree as ET
from lxml import etree as ET
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import numpy as np

config = ("Driver={ODBC Driver 17 for Sql Server};"
          "Server=COSMOS;"
          "Database=COSMOS_V14B;"
          "Trusted_Connection=Yes;")

url = quote_plus(config)
conn = create_engine('mssql+pyodbc:///?odbc_connect=%s' % url)
ns = "{http://www.portalfiscal.inf.br/nfe}"

COLUNAS = ['cProd', 'cEAN',
           'CFOP', 'uCom', 'qCom', 'vUnCom', 'vDesc', 'vProd',
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
            'Serie': np.int64, 'Deposito': np.int64,
            'dhEmi': np.datetime64, 'Id': np.int64,
            'cProd': np.int64}


def retorna_chave():
    with conn.connect() as abrir:
        rst = abrir.execute(
            '''
               select top 1
                  x1.dscXml
               from dbnfe.dbo.tbNfeXml x1
                where x1.controle1 like 'DEV%'
            '''
        ).scalar()

    if rst is not None:
        root = ET.fromstring(rst)
        for doc in root.iter():
            doc.tag = doc.tag[len(ns):]
        return root
    else:
        return None


def dados(root):

    for child in root.findall("infNFe/det"):
        linhas = dict()
        linhas['Deposito'] = root.find('infNFe/emit/CNPJ').text[8:12]
        linhas['natOp'] = root.find('infNFe/ide/natOp').text

        dt_emiss = root.find('infNFe/ide/dhEmi')
        if dt_emiss is None:
            linhas['dhEmi'] = root.find('infNFe/ide/dEmi').text[0:10]
        else:
            linhas['dhEmi'] = dt_emiss.text[0:10]

        linhas['chaveAcesso'] = root.find('infNFe').get('Id')[3:]
        linhas['nNF'] = root.find('infNFe/ide/nNF').text
        linhas['vNF'] = root.find('infNFe/total//vNF').text
        linhas['vICMS_nota'] = root.find('infNFe/total//vICMS').text
        linhas['Serie'] = root.find('infNFe/ide/serie').text

        for element in child.iter():
            if element.tag == 'det':
                linhas[element.tag] = element.get('nItem')

            if element.tag in COLUNAS:
                # VERIFICA SE A COLUNA VBC É DO [ICMS. IPI, COFINS]
                if element.tag == 'vBC':
                    if element.getparent().getparent().tag == 'ICMS':
                        linhas['ICMS_' + element.tag] = element.text
                    if element.getparent().getparent().tag == 'PIS':
                        linhas['PIS_' + element.tag] = element.text
                    if element.getparent().getparent().tag == 'COFINS':
                        linhas['COFINS_' + element.tag] = element.text
                else:
                    linhas[element.tag] = element.text

        yield linhas.copy()


root = retorna_chave()

# tratamento do data frame
df_comp = pd.DataFrame(list(dados(root)))
conversoes = {k: COL_TIPO[k] for k in df_comp.keys() if k in COL_TIPO.keys()}
df_comp = df_comp.astype(conversoes)

df_comp.to_excel('Dados.xlsx', index=False)
