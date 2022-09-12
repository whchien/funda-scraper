import sqlite3
from scraper.config import config
from scraper.preprocess import preprocess_data
from scraper.utils import logger
import pandas as pd


class DBManager:
    def __init__(self, db_name: str = "funda.db"):
        self.conn = sqlite3.connect(db_name)
        self.init()

    def check_table_exist(self, table_name: str) -> bool:
        c = self.conn.cursor()
        c.execute(
            """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name=(?)""",
            (table_name,),
        )
        return True if c.fetchone()[0] == 1 else False

    def init_table(self, is_raw: str = True):
        if is_raw:
            col_names = list(config.css_selector.keys())
            col_names += ["city", "log_id"]
            table_name = "raw"
        else:
            col_names = list(config.keep_cols.sold_data + config.keep_cols.selling_data)
            table_name = "clean"

        df = pd.DataFrame({c: [] for c in col_names})
        df.to_sql(table_name, self.conn, if_exists="replace")
        logger.info(f"Table created: {table_name}")

    def init(self) -> None:
        self.init_table(is_raw=True)
        self.init_table(is_raw=False)

    def write_raw(self, df: pd.DataFrame) -> None:
        self.write_df(df, "raw")

    def write_df(self, df: pd.DataFrame, table_name: str) -> None:
        if table_name not in ["raw", "clean"]:
            logger.warning(
                "Only 2 tables in default. "
                "Table names other than 'raw' or 'clean' might cause errors."
            )

        mode = "append" if self.check_table_exist(table_name) else "replace"
        df.to_sql(table_name, self.conn, if_exists=mode)
        logger.info(f"Data written in table {table_name}: {df.shape[0]}")

    def query(self, query: str) -> pd.DataFrame:
        c = self.conn.cursor()
        result = c.execute(f"""{query}""")
        cols = [column[0] for column in result.description]
        result_df = pd.DataFrame.from_records(data=result.fetchall(), columns=cols)
        return result_df

    def clean_raw(self) -> None:
        raw_df = self.query("SELECT * FROM raw")
        clean_df = preprocess_data(raw_df)
        self.write_df(clean_df, "clean")
        logger.info(f"Raw data are preprocessed and stored in table 'clean'.")


if __name__ == "__main__":
    manager = DBManager()
    df = pd.read_csv("./data/raw/close/houseprice_20220605_amsterdam_sold_buy_16.csv")
    df = preprocess_data(df)
    manager.write_df(df, "clean")
