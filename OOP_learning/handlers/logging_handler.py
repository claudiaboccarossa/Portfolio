import logging
from pathlib import Path
from OOP_learning.handlers.config_handler import ConfigHandler


class LoggingHandler:
    def __init__(self, name_file, now):
        """
        Classe per la gestione dei log.
        :param name_file: Nome del file python che sta utilizzando il logger.
        :param now: variabile di timestamp per poter avere lo stesso log per ogni run.
        """
        try:
            self.logger = None
            self.config = ConfigHandler()
            # Parametri dal config.ini
            self.time = self.config.get_param('LOGGING', 'timestamp')
            if self.time == 'true':
                timestamp = str(now.strftime('%Y%m%d%H%M%S'))
                log_file_raw = Path(self.config.get_param('LOGGING', 'file_path_log'))
                self.log_file = f'{log_file_raw}_{timestamp}.log'
            self.formatter = self.config.get_param('LOGGING', 'format_name')
            self.encoding = self.config.get_param('LOGGING', 'encoding')
            self.mode = self.config.get_param('LOGGING', 'mode')

            # Creo la directory se non esiste
            log_file_raw.parent.mkdir(parents=True, exist_ok=True)

            # Configuro il logger
            self.logger = logging.getLogger(name_file)
            self.logger.setLevel(logging.DEBUG)

            # Evita duplicati di handler
            if not self.logger.handlers:
                file_handler = logging.FileHandler(
                    self.log_file, mode=self.mode, encoding=self.encoding
                )
                formatter = logging.Formatter(self.formatter)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

        except Exception as e:
            print(f"[ERRORE LOGGING] {e}")
            self.logger = None

    def get_logger(self):
        """
        :return: logger
        """
        if self.logger is None:
            raise RuntimeError("Il logger non è stato inizializzato correttamente.")
        return self.logger


if __name__ == "__main__":
    pass