from pathlib import Path
import pandas as pd
from OOP_learning.handlers.config_handler import ConfigHandler
from OOP_learning.handlers.logging_handler import LoggingHandler


class Dataframe_utils:
    def __init__(self, now):
        """
        Classe per la gestione del dataframe dal caricamento del dataset al salvataggio su filesystem
        :param now: timestamp per la gestione dei log.
        """
        self.config = ConfigHandler()
        log_handler = LoggingHandler(__name__, now)
        self.logger = log_handler.get_logger()

    def load_df(self):
        '''
        Legge il csv indicato nel file ini 'file_path' alla sezione 'FILE_CONFIG'
        :return: Ritorna il dataframe che utilizzerà il programma per l'ETL e l'analisi
        '''
        self.file_path = Path(self.config.get_param('FILE_CONFIG', 'file_path'))

        if self.file_path.suffix.lower() == '.csv':
            delimiter = self.config.get_param('FILE_CONFIG', 'delimiter')
            encoding = self.config.get_param('FILE_CONFIG', 'encoding')
            header = int(self.config.get_param('FILE_CONFIG', 'has_header'))
            self.df = pd.read_csv(self.file_path, delimiter=delimiter, encoding=encoding, header=header)
            self.logger.info(f"Loaded {len(self.df)} records from {self.file_path}")

        elif self.file_path.suffix.lower() == '.xml':
            self.df = pd.read_xml(self.file_path)
            self.logger.info(f"Loaded {len(self.df)} records from {self.file_path}")

        elif self.file_path.suffix.lower() == '.json':
            self.df = pd.read_json(self.file_path)
            self.logger.info(f"Loaded {len(self.df)} records from {self.file_path}")

        elif self.file_path.suffix.lower() == '.xlsx':
            self.df = pd.read_excel(self.file_path)
            self.logger.info(f"Loaded {len(self.df)} records from {self.file_path}")

        return self.df

    def save_df(self, df, out_path: str):
        '''
        salva il df passato come parametro come file csv o xml a seconda del suffisso scritto nel file ini
        alla voce 'output_path' nella sezione 'SAVING_CONFIG'
        :param df: dataframe da salvare
        :param out_path: parametro sul config.ini 'output_path' nella sezione 'SAVING_CONFIG'
        :return:
        '''
        self.output_path = Path(self.config.get_param('SAVING_CONFIG', 'output_path'))
        self.output_path.mkdir(parents=True, exist_ok=True)

        if self.output_path.suffix.lower() == '.csv':
            df.to_csv(out_path, index=False)
            self.logger.info(f"Saved {len(self.df)} records to {self.output_path}")

        if self.output_path.suffix.lower() == '.xml':
            df.to_xml(out_path, index=False)
            self.logger.info(f"Saved {len(self.df)} records to {self.output_path}")

