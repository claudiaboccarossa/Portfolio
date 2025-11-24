from OOP_learning.handlers.logging_handler import LoggingHandler
from OOP_learning.handlers.config_handler import ConfigHandler
from OOP_learning.core.utils import read_csv, clean_data, parse_cols
import pandas as pd


class DataProcessing:
    def __init__(self, df, now, dropN=False):
        """
        Classe per ETL dei dataset.
        :param df: DataFrame che contiene il dataset da trasformare
        :param now: timestamp per gestione dei log.
        :param dropN: parametro nominale che permette di droppare null o meno (di Default è in false)
        """
        log_handler = LoggingHandler(__name__, now)
        self.logger = log_handler.get_logger()
        self.logger.info("DataProcessing")
        self.config = ConfigHandler()
        self.file_path = self.config.get_param('FILE_CONFIG', 'file_path')
        self.df = df

        if dropN:
            self.logger.info("DataProcessing: Pulizia Null..")
            self.df = clean_data(self.df)
            self.logger.info("DataProcessing: Pulizia Null terminata")

    def typify_data(self):
        """
        Metodo per tipizzare le colonne del dataset.
        :return: il dataframe con le colonne tipizzate come da indicazione su file ini.
        """
        try:
            int_cols = parse_cols(self.config.get_param('ETL', 'integers'))
            float_cols = parse_cols(self.config.get_param('ETL', 'floats'))
            bool_cols = parse_cols(self.config.get_param('ETL', 'bools'))
            str_cols = parse_cols(self.config.get_param('ETL', 'strings'))

            if int_cols:
                self.logger.info("DataProcessing: Integer columns detected")
                for col in int_cols:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('Int64')
                self.logger.info(f"Tipizzate a intero le colonne: {int_cols}")

            if float_cols:
                self.logger.info("DataProcessing: Float columns detected")
                for col in float_cols:
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                self.logger.info(f"Tipizzate a float le colonne: {float_cols}")

            if bool_cols:
                self.logger.info("DataProcessing: Boolean columns detected")
                for col in bool_cols:
                    self.df[col] = self.df[col].astype('boolean')
                self.logger.info(f"Tipizzate a booleano le colonne: {bool_cols}")

            if str_cols:
                self.logger.info("DataProcessing: String columns detected")
                for col in str_cols:
                    self.df[col] = self.df[col].astype(str)
                self.logger.info(f"Tipizzate a stringa le colonne: {str_cols}")

            return self.df

        except Exception as e:
            self.logger.error(f"Errore durante la tipizzazione dei dati: {e}")


    def join_tables(self):
        """
        Metodo per fare join su due tabelle del db.
        :return: Query per creare una vista con la join su db.
        """

        try:
            flg_join = self.config.get_param('ETL', 'flg_join')
            if flg_join == '1':
                self.logger.info("DataProcessing: Joining tables")
                # TODO: Dare la possibilità di scegliere tra view e table
                view = parse_cols(self.config.get_param('ETL', 'view'))
                tables = parse_cols(self.config.get_param('ETL', 'tables'))
                join = parse_cols(self.config.get_param('ETL', 'join'))
                key = parse_cols(self.config.get_param('ETL', 'key'))
                select_l = parse_cols(self.config.get_param('ETL', 'select_l'))
                select_r = parse_cols(self.config.get_param('ETL', 'select_r'))
                schema = parse_cols(self.config.get_param('DB_INTERACTIONS', 'schema'))

                l_list = ','.join([f" {tables[0]}.{col}" for col in select_l])
                r_list = ','.join([f" {tables[1]}.{col}" for col in select_r])

                join_query = (f"""CREATE VIEW {schema[0]}.{view[0]} AS (
                            SELECT {l_list}, {r_list} FROM {schema[0]}.{tables[0]}
                            {join[0]} JOIN {schema[0]}.{tables[1]}
                             ON {tables[0]}.{key[0]} = {tables[1]}.{key[0]})""")
                return join_query

        except Exception as e:
            self.logger.error(f"Errore durante la costruzione della query di join: {e}")


    def calculated_cols(self):
        """
        Metodo per creare colonne calcolate sul dataset.
        :return: DataFrame con le nuove colonne calcolate.
        """
        try:
            flg_calc = self.config.get_param('ETL', 'flg_calc')
            if flg_calc == '1':
                self.logger.info("DataProcessing: Calculated columns")
                name_m = self.config.get_param('ETL', 'new_col_m_name')
                name_s = self.config.get_param('ETL', 'new_col_s_name')
                name_d = self.config.get_param('ETL', 'new_col_d_name')

                if name_m != '':
                    cols = parse_cols(self.config.get_param('ETL', 'multiply'))
                    self.df[name_m] = self.df[cols].prod(axis=1)
                    self.logger.info(f"Creata nuova colonna calcolata: {name_m}")

                if name_s != '':
                    cols = parse_cols(self.config.get_param('ETL', 'sum'))
                    self.df[name_s] = self.df[cols].sum(axis=1)
                    self.logger.info(f"Creata nuova colonna calcolata: {name_s}")

                if name_d != '':
                    dis = int(self.config.get_param('ETL', 'discount'))
                    col = self.config.get_param('ETL', 'which_column')
                    self.df[f'sconto_{dis}'] = (dis * self.df[col]) / 100
                    self.df[name_d] = self.df[col] - (( dis * self.df[col]) / 100)
                    self.logger.info(f"Creata nuova colonna calcolata: {name_d}")

            return self.df

        except Exception as e:
            self.logger.error(f"Errore durante la creazione delle colonne calcolate: {e}")

















