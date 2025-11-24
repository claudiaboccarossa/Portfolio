import configparser
from pathlib import Path


class ConfigHandler:
    """
    Classe per leggere la configurazione su file.ini.
    Il parametro nominale prende il file config.ini di default.
    """
    def __init__(self, ini_path=None):
        self.config = configparser.ConfigParser(interpolation=None)

        base_dir = Path(__file__).parent.parent
        self.ini_path = Path(ini_path) if ini_path else base_dir / "config.ini"

    def load_config(self):
        """
        Carica il file ini indicato al path della classe.
        :return: il file ini letto formato testo.
        """
        files_read = self.config.read(self.ini_path, encoding="utf-8")
        if not files_read:
            raise FileNotFoundError(f"File di configurazione non trovato: {self.ini_path}")
        return self.config

    def get_param(self, section, key):
        """
        Metodo per leggere il valore dei parametri nel file ini
        :param section: sezione a cui appartiene il parametro
        :param key: chiave del parametro
        :return: valore parametro
        """
        if not self.config.sections():
            self.load_config()
        return self.config[section][key]


def main():
    config = ConfigHandler()
    path_log = config.get_param("LOGGING", "file_path_log")
    print(path_log)

if __name__ == "__main__":
    main()
