import pandas as pd
from sqlalchemy import create_engine, text
import os
import dotenv
from logging_config import logger
from typing import Literal

dotenv.load_dotenv()
dotenv_path = dotenv.find_dotenv()

class connector:
    def __init__(self, schema, user=os.getenv("POSTGRES_DB_USERNAME"), password=os.getenv("POSTGRES_DB_PASSWORD"), address='localhost', port=5432, db_name='investment_advise_platform'):
        if not user:
            user = input("Enter your PostgreSQL username: ").strip()
            dotenv.set_key(dotenv_path, "POSTGRES_DB_USERNAME", user)
        if not password:
            password = input("Enter your PostgreSQL password: ").strip()
            dotenv.set_key(dotenv_path, "POSTGRES_DB_PASSWORD", password)
        self.engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{address}:{port}/{db_name}')
        self.connection = self.engine.connect()
        self.connection.execute(text(f"SET search_path TO {schema}"))
        self.db_name = db_name
        self.schema = schema

    def query_data(self, query: str):
        cursor = self.connection.execute(text(query))
        result = cursor.fetchall()
        return result
    
    def query_dataframe(self, query: str) -> pd.DataFrame:
        """Executes a SQL query and returns the result as a DataFrame."""
        try:
            df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def query_columns(self, table_name: str) -> list:
        """Returns a list of column names for the specified table."""
        try:
            query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{self.schema}' ORDER BY ordinal_position;"
            df = pd.read_sql_query(query, self.connection)
            return df['column_name'].tolist()
        except Exception as e:
            logger.error(f"Error fetching columns for table '{table_name}': {e}")
            return []
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: Literal['fail', 'replace', 'append'] = "append", chunksize: int|None = None) -> None:
        """
        Inserts a DataFrame into the specified SQL table.
        Supports chunked insertion for large DataFrames via the chunksize parameter.
        """
        try:
            filtered_df = df[[col for col in df.columns if col in self.query_columns(table_name)]]
            if chunksize is not None:
                filtered_df.to_sql(table_name, self.engine, schema=self.schema, if_exists=if_exists, index=False, chunksize=chunksize)
            else:
                filtered_df.to_sql(table_name, self.engine, schema=self.schema, if_exists=if_exists, index=False)
            logger.info(f"Successfully inserted DataFrame into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error inserting DataFrame into table '{table_name}': {e}")

    def insert_record(self, table_name: str, record: dict) -> None:
        """
        Inserts a single record (as a dict) into the specified SQL table.
        """
        try:
            df = pd.DataFrame([record])
            self.insert_dataframe(df, table_name)
            logger.info(f"Successfully inserted record into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error inserting record into table '{table_name}': {e}")
    
    def get_snapshot_id(self,) -> int:
        cursor = self.connection.execute(text("INSERT INTO ods.snapshot DEFAULT VALUES RETURNING id;"))
        row = cursor.fetchone()
        if row is None:
            logger.error("No snapshot ID returned from database.")
            return -1
        return row[0]
    
    def update_record(self, table_name: str, update_values: dict, conditions: dict) -> None:
        """
        Updates record(s) in the specified SQL table.

        Args:
            table_name (str): The name of the table to update.
            update_values (dict): A dict of {column: new_value} to update.
            conditions (dict): A dict of {column: value} used in the WHERE clause.
        """
        try:
            update_values.pop(conditions.keys(), None) 
            set_clause = ", ".join([f"{col} = :{col}" for col in update_values.keys()])
            where_clause = " AND ".join([f"{col} = :cond_{col}" for col in conditions.keys()])

            params = {**update_values, **{f"cond_{col}": val for col, val in conditions.items()}}
            query = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};")

            self.connection.execute(query, params)
            self.connection.commit()
            logger.info(f"Successfully updated records in '{table_name}'.")
        except Exception as e:
            logger.error(f"Error updating records in table '{table_name}': {e}")

    def update_from_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: list[str],
        update_columns: list[str] | None = None,
    ) -> None:
        """
        Bulk UPDATE `table_name` using rows from `df`.
        - `key_columns`: columns used to match existing rows (e.g., PK/composite PK).
        - `update_columns`: columns to update (defaults to df columns minus keys).
        """
        try:
            # Validate columns against actual table
            table_cols = set(self.query_columns(table_name))
            if not table_cols:
                raise ValueError(f"Could not fetch columns for '{table_name}'.")
            if not set(key_columns).issubset(table_cols):
                missing = set(key_columns) - table_cols
                raise ValueError(f"Key columns not in table: {missing}")

            # Decide which columns to update
            if update_columns is None:
                update_columns = [c for c in df.columns if c not in key_columns]
            update_columns = [c for c in update_columns if c in table_cols and c not in key_columns]
            if not update_columns:
                logger.info("No updatable columns found; nothing to do.")
                return

            # Keep only needed columns
            cols_needed = [c for c in (key_columns + update_columns) if c in df.columns]
            work = df.loc[:, cols_needed].drop_duplicates(subset=key_columns)
            if work.empty:
                logger.info("Input DataFrame is empty after filtering; nothing to do.")
                return

            # Create TEMP table and insert DataFrame
            temp_name = f"tmp_update_{table_name}"
            with self.connection.begin():
                self.connection.execute(text(f"DROP TABLE IF EXISTS {temp_name};"))
                work.to_sql(temp_name, self.engine, if_exists="replace", index=False)

                # Build UPDATE statement
                set_sql = ", ".join([f"t.{c} = s.{c}" for c in update_columns])
                join_sql = " AND ".join([f"t.{k} = s.{k}" for k in key_columns])

                self.connection.execute(text(f"""
                    UPDATE {table_name} AS t
                    SET {set_sql}
                    FROM {temp_name} AS s
                    WHERE {join_sql};
                """))

                self.connection.execute(text(f"DROP TABLE IF EXISTS {temp_name};"))

            logger.info(f"Bulk update completed for '{table_name}'.")
        except Exception as e:
            logger.error(f"Bulk update error for '{table_name}': {e}")

if __name__ == "__main__":
    db = connector(schema='ods')
    query = "SELECT * FROM information_schema.tables LIMIT 10;"
    df = db.query_dataframe(query)
    # print(df.head())
    # df = pd.read_csv(os.path.expanduser('~/Documents/watch_list.csv'))
    # db_ods = connector(schema='ods')
    # db_ods.insert_dataframe(df, 'watch_list', if_exists='append')
    # cols = db_ods.query_columns('watch_list')
    # df = df[[col for col in df.columns if col in cols]]