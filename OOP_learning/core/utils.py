from pathlib import Path
import pandas as pd


def convert_to_windows_path(relative_path: str) -> Path:
    absolute_path = Path(relative_path).resolve()
    return absolute_path

'''legge da un csv al path indicato'''
def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

'''scrive su file csv il df passato come parametro'''
def write_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

'''droppa i null del df passato come parametro'''
def clean_data(df):
    df = df.dropna()
    return df

'''parsa una stringa e restitusice una lista grazie al separatore virgola'''
def parse_cols(cols_string):
    if not cols_string:
        return []
    return [c.strip() for c in cols_string.split(',')]