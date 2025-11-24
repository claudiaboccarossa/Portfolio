import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
from OOP_learning.handlers.config_handler import ConfigHandler
from OOP_learning.handlers.logging_handler import LoggingHandler

class DBConnector:
    def __init__(self, now):
        """
        Classe per collegamento e interazione su DBMS.
        :param now: timestamp per gestione dei log.
        """
        log_handler = LoggingHandler(__name__, now)
        self.logger = log_handler.get_logger()

        self.config = ConfigHandler()
        self.host = self.config.get_param('DB_INFO', 'host')
        self.port = self.config.get_param('DB_INFO', 'port')
        self.user = self.config.get_param('DB_INFO', 'user')
        self.psw = self.config.get_param('DB_INFO', 'psw')
        self.conn = None

    def db_connect(self):
        """
        Metodo di collegamento al db, sfrutta i parametri del file ini.
        :return: la connessione per poter interagire con il db.
        """
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.psw,
            )
            if self.conn.is_connected():
                self.logger.info("Connessione al database riuscita!")
        except Error as e:
            self.logger.error(f"Errore durante la connessione: {e}")
        return self.conn

    def create_schema(self):
        """
        Metodo per la creazione di nuovi schema su DB.
        Sfrutta i parametri del file ini.
        :return: /
        """
        conn = self.db_connect()
        self.schema = self.config.get_param('DB_INTERACTIONS', 'schema')
        if conn and self.schema:
            try:
                cursor = conn.cursor()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{self.schema}`")
                self.logger.info(f"Schema '{self.schema}' creato (o già esistente).")
            except Exception as e:
                self.logger.error(f"Errore durante la creazione dello schema: {e}")
            finally:
                cursor.close()
                conn.close()

    def create_tables(self, df):
        """
        Crea nuove tabelle su DB sfruttando i parametri su file ini.
        :param df: Dataframe contenente il dataset da caricare sulla nuova tabella.
        :return: /
        """
        if not self.conn or not self.conn.is_connected():
            self.db_connect()
        self.schema = self.config.get_param('DB_INTERACTIONS', 'schema')
        table = self.config.get_param('DB_INTERACTIONS', 'table1')

        if self.conn and table and not df.empty:
            try:
                engine = create_engine(f"mysql+mysqlconnector://{self.user}:{self.psw}@{self.host}/{self.schema}")
                df.to_sql(f"{table}", con=engine, index=False, if_exists='replace')
                self.logger.info(f"Tabella '{table}' creata con successo ({len(df)} righe).")

            except Exception as e:
                self.logger.error(f"Errore durante la creazione della tabella {table}: {e}")

            finally:
                self.conn.close()

    def drop_schema(self):
        """
        Metodo che cancella gli schema su DB.
        Sfrutta i parametri del file ini.
        :return: /
        """
        if not self.conn or not self.conn.is_connected():
            self.db_connect()
        self.schema = self.config.get_param('DB_INTERACTIONS', 'drop_schema')

        if self.conn and self.schema != '':
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"DROP SCHEMA {self.schema}")
                self.logger.info(f"Schema '{self.schema}' eliminato con successo.")

            except Exception as e:
                self.logger.error(f"Errore durante l'eliminazione dello schema: {e}")
            finally:
                cursor.close()
                self.conn.close()
                self.logger.info("Connessione al database chiusa!")
        else:
            self.logger.info(f"Nessuno schema da eliminare trovato. Aggiornare il file di configurazione alla voce 'drop_schema' se si desidera procedere.")

    def sql_query(self, query):
        """
        Metodo che esegue qualsiasi query sul db.
        :param query: Stringa contenente la query da eseguire.
        :return: /
        """

        try:
            if not self.conn or not self.conn.is_connected():
                self.db_connect()
            cursor = self.conn.cursor()
            self.logger.info(f"Esecuzione query: \n{query}")
            cursor.execute(query)

        except Exception as e:
            self.logger.error(f"Errore durante la query: {query}\nERROR: {e}")

        finally:
            cursor.close()
            self.conn.close()

# def main():
#
#     try:
#       conn = DBConnector()
#       conn.db_connect()
#
#     except Exception as e:
#         logger.error(f"ERRORE NEL DB CONNECTOR: {e}")
#
#
# if __name__ == "__main__":
#     main()