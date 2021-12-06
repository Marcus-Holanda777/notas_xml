from sqlalchemy import create_engine
from urllib.parse import quote_plus
from collections import namedtuple
from datetime import datetime

Notas = namedtuple('Notas', ['chave', 'string_xml', 'controle', 'isnStatus'])


class Conexao:
    config = ("Driver={};"
              "Server=COSMOS;"
              "Database={};"
              "Trusted_Connection=Yes;")

    def __init__(self) -> None:
        self.__driver = "{ODBC Driver 17 for Sql Server}"
        self.__banco = 'dbnfe'
        self.config_conf = self.config.format(self.driver, self.banco)
        self.url = quote_plus(self.config_conf)

    @property
    def driver(self):
        return self.__driver

    @property
    def banco(self):
        return self.__banco

    @banco.setter
    def banco(self, value: str):
        if value.lower() == 'dbnfe' or value.lower() == 'dbnfeloja':
            self.__banco = value
            self.config_conf = self.config.format(self.driver, self.__banco)
            self.url = quote_plus(self.config_conf)
        else:
            raise ValueError("[ERRO] Banco de dados não localizado !")

    def conectar(self):
        return create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.url)


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


if __name__ == '__main__':
    c = Conexao().conectar()

    notas = ListaNotas.chave(
        chave='chave_de_acesso', conn=c)
    for x in notas:
        print(x)
        break
