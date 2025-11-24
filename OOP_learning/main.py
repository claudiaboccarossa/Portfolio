import datetime
from OOP_learning.core.analysis import Analysis
from OOP_learning.core.data_processing import DataProcessing
from OOP_learning.core.db_connector import DBConnector
from OOP_learning.core.models import Dataframe_utils
from OOP_learning.handlers.logging_handler import LoggingHandler

def main():
    now = datetime.datetime.now() # variabile che mi permette di avere lo stesso timestamp sui log ad ogni Run
    log_handler = LoggingHandler(__name__, now)
    logger = log_handler.get_logger()

    try:
        logger.info(f"*************** Starting program ***************")

        # LOADING DATA
        model = Dataframe_utils(now)
        df = model.load_df()

        # ETL: Transformation
        etl = DataProcessing(df, now)
        df = etl.typify_data()
        df = etl.calculated_cols()

        # ETL: Join
        view_query = etl.join_tables()

        # SCRITTURA SUL DB
        # -connessione al DB
        conn = DBConnector(now)

        # -crea/elimina schema
        conn.create_schema()
        conn.drop_schema()

        # -crea tabelle/viste
        conn.create_tables(df)
        conn.sql_query(query=view_query)

        # ANALYSIS
        analyze = Analysis(df, now)
        # -group by
        gb_query = analyze.grouped_by()
        conn.sql_query(query=gb_query) #rank_quantity_by_city
        # -graph
        analyze.grouped_by()

    except RuntimeError as e:
        logger.error("------------------------------------------------")
        logger.error(f"ERRORE runtime nel main: {e}")

    except Exception as e:
        logger.error(f"ERRORE NEL MAIN: {e}")

    finally:
        logger.info(f"*************** Ending program ***************\n")


if __name__ == "__main__":
    main()
