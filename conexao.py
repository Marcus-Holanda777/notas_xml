from sqlalchemy import create_engine
from urllib.parse import quote_plus


class Conexao:
    config = ("Driver={};"
              "Server={};"
              "Database={};"
              "Trusted_Connection=Yes;")

    def __init__(self, server, database) -> None:
        self.__driver = "{ODBC Driver 17 for Sql Server}"
        self.__server = server
        self.__banco = database
        self.config_conf = self.config.format(
            self.driver, self.server, self.banco)
        self.url = quote_plus(self.config_conf)

    @property
    def driver(self):
        return self.__driver

    @property
    def banco(self):
        return self.__banco

    @property
    def server(self):
        return self.__server

    @banco.setter
    def banco(self, value: str):
        self.__banco = value
        self.config_conf = self.config.format(
            self.driver, self.server,  self.banco)
        self.url = quote_plus(self.config_conf)

    def conectar(self):
        return create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.url)
