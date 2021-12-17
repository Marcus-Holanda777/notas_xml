from typing import List
from lxml import etree
from collections import namedtuple
from datetime import datetime

Notas = namedtuple('Notas', ['chave', 'string_xml', 'controle', 'isnStatus'])


class XmlNota:
    def __init__(self, nota, colunas: List, ns="{http://www.portalfiscal.inf.br/nfe}") -> None:
        self.nota = nota
        self.ns = ns
        self.colunas = colunas
        self.root = self.remover_namespace()

    def remover_namespace(self):
        root = etree.fromstring(self.nota.string_xml)
        for doc in root.iter():
            doc.tag = doc.tag[len(self.ns):]

        return root

    def dados(self):
        for child in self.root.findall("infNFe/det"):
            linhas = dict()
            linhas['isnStatus'] = self.nota.isnStatus
            linhas['controle'] = self.nota.controle
            linhas['cnpjEmi'] = self.root.find('infNFe/emit/CNPJ').text[8:12]
            linhas['natOp'] = self.root.find('infNFe/ide/natOp').text

            dt_emiss = self.root.find('infNFe/ide/dhEmi')
            if dt_emiss is None:
                linhas['dhEmi'] = self.root.find('infNFe/ide/dEmi').text[0:10]
            else:
                linhas['dhEmi'] = dt_emiss.text[0:10]

            linhas['chaveAcesso'] = self.root.find('infNFe').get('Id')[3:]

            # Verificar se é nota de estorno
            if self.nota.controle.lower().startswith('estorno'):
                linhas['chaveEstorno'] = self.root.find(
                    'infNFe/ide//refNFe').text

            linhas['nNF'] = self.root.find('infNFe/ide/nNF').text
            linhas['vNF'] = self.root.find('infNFe/total//vNF').text
            linhas['vICMS_nota'] = self.root.find('infNFe/total//vICMS').text
            linhas['Serie'] = self.root.find('infNFe/ide/serie').text

            for element in child.iter():
                if element.tag == 'det':
                    linhas[element.tag] = element.get('nItem')

                if element.tag in self.colunas:
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


class ListaNotas:
    @staticmethod
    def periodo(inicio: datetime, fim: datetime, conn):
        select = f'''
            select
                     x1.codChaveAcesso as chave
                    ,x1.dscXml as string_xml
                    ,x1.controle1 as controle
                    ,x1.isnStatus
            from tbNfeXml x1
            where x1.dthGravacao
            between '{inicio.strftime('%Y-%m-%d')} 00:00:00' and  '{fim.strftime('%Y-%m-%d')} 23:59:59'
            and x1.dscXml is not null
            and x1.isnStatus != 3
        '''

        with conn.connect() as abrir:
            rst = abrir.execute(
                select
            )

            for row in map(Notas._make, rst):
                yield row

    @staticmethod
    def chave(conn, *chave):
        select = f'''
            select
                     x1.codChaveAcesso as chave
                    ,x1.dscXml as string_xml
                    ,x1.controle1 as controle
                    ,x1.isnStatus
            from tbNfeXml x1
            where x1.codChaveAcesso in('{"','".join(chave)}')
            and x1.dscXml is not null
            and x1.isnStatus != 3
        '''

        with conn.connect() as abrir:
            rst = abrir.execute(
                select
            )

            for row in map(Notas._make, rst):
                yield row

    @staticmethod
    def controle(conn, *controle, **dados):
        comp_where = ''
        if dados:
            comp_where = f'''and x1.dthGravacao 
            between '{dados['inicio'].strftime('%Y-%m-%d')} 00:00:00'
            and '{dados['fim'].strftime('%Y-%m-%d')} 23:59:59' '''

        select = f'''
            select
                     x1.codChaveAcesso as chave
                    ,x1.dscXml as string_xml
                    ,x1.controle1 as controle
                    ,x1.isnStatus
            from tbNfeXml x1
            where x1.controle1 in('{"','".join(controle)}')
            {comp_where}
            and x1.dscXml is not null
            and x1.isnStatus != 3
        '''

        print(select)

        with conn.connect() as abrir:
            rst = abrir.execute(
                select
            )

            for row in map(Notas._make, rst):
                yield row
