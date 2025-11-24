from OOP_learning.handlers.logging_handler import LoggingHandler
from OOP_learning.handlers.config_handler import ConfigHandler
import matplotlib.pyplot as plt


class Analysis:
    def __init__(self, df, now):
        """
        Classe di analisi dataset
        :param df: dataframe dove è caricato il dataset da analizzare.
        :param now: timestamp per la gestione dei log.
        """
        log_handler = LoggingHandler(__name__, now)
        self.logger = log_handler.get_logger()
        self.logger.info("Analysis started")
        self.df = df
        self.config = ConfigHandler()

    def grouped_by(self):
        """
        Metodo per fare group by dei dataset.
        :return: La query con il group by desiderato per poter caricare viste su db.
        """
        try:
            flg_gb = self.config.get_param('ANALYSIS', 'flg_gb')

            if flg_gb == "1":
                table_gb = self.config.get_param('ANALYSIS', 'table_gb')
                name_gb = self.config.get_param('ANALYSIS', 'name_gb')
                group_by = self.config.get_param('ANALYSIS', 'group_by')
                aggr = self.config.get_param('ANALYSIS', 'aggr')
                aggr_col = self.config.get_param('ANALYSIS', 'aggr_col')
                self.logger.info(f"Esecuzione group by: {group_by}")

                if group_by == aggr_col:
                    alias = f"n_{aggr_col}"
                else:
                    alias = aggr_col

                query = f"""CREATE VIEW {name_gb} AS (
                SELECT {group_by}, {aggr}({aggr_col}) as {alias} FROM {table_gb}
                GROUP BY {group_by} ORDER BY {aggr_col} DESC
                )"""

                return query

            else:
                self.logger.info(f"Group by non eseguito. Compilare la sezione 'GROUP BY' in 'ANALYSIS' se si vuole procedere.")

        except Exception as e:
            self.logger.error(f"Errore durante la creazione della query di group by: {e}")

    def graph(self, x, y):
        # TODO: finire lo svilutto e inserire commento
        plt.bar(x, y)
        plt.title("Grafico a Linee Personalizzato")
        plt.xlabel("Asse X")
        plt.ylabel("Asse Y")
        plt.grid(True)
        plt.show()