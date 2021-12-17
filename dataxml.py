from typing import List
from lxml import etree


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
