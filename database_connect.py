import os
import uuid
from typing import Literal

import dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

from logging_config import logger


dotenv.load_dotenv()
dotenv_path = dotenv.find_dotenv()


class connector:
    def __init__(
        self,
        schema,
        user=os.getenv("POSTGRES_DB_USERNAME"),
        password=os.getenv("POSTGRES_DB_PASSWORD"),
        address="localhost",
        port=5432,
        db_name="investment_advise_platform",
    ):
        if not user:
            user = input("Enter your PostgreSQL username: ").strip()
            dotenv.set_key(dotenv_path, "POSTGRES_DB_USERNAME", user)
        if not password:
            password = input("Enter your PostgreSQL password: ").strip()
            dotenv.set_key(dotenv_path, "POSTGRES_DB_PASSWORD", password)

        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{address}:{port}/{db_name}"
        )
        self.connection = self.engine.connect()
        self.connection.execute(text(f"SET search_path TO {schema}"))
        self.db_name = db_name
        self.schema = schema

    def execute(self, query: str, params: dict | None = None, commit: bool = True):
        cursor = self.connection.execute(text(query), params or {})
        if commit:
            self.connection.commit()
        return cursor

    def query_data(self, query: str, params: dict | None = None):
        cursor = self.connection.execute(text(query), params or {})
        return cursor.fetchall()

    def query_dataframe(self, query: str, params: dict | None = None) -> pd.DataFrame:
        try:
            return pd.read_sql_query(text(query), self.connection, params=params or {})
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return pd.DataFrame()

    def query_columns(self, table_name: str) -> list[str]:
        try:
            query = text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                  AND table_schema = :table_schema
                ORDER BY ordinal_position;
                """
            )
            df = pd.read_sql_query(
                query,
                self.connection,
                params={"table_name": table_name, "table_schema": self.schema},
            )
            return df["column_name"].tolist()
        except Exception as e:
            logger.error(f"Error fetching columns for table '{table_name}': {e}")
            return []

    def _filter_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        cols_in_db = self.query_columns(table_name)
        filtered_columns = [col for col in df.columns if col in cols_in_db]
        if not filtered_columns:
            return pd.DataFrame()
        return df[filtered_columns].copy()

    def _infer_dtypes(self, df: pd.DataFrame) -> dict:
        return {
            column: JSONB
            for column in df.columns
            if column.endswith("_payload") or column == "source_payload"
        }

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: Literal["fail", "replace", "append"] = "append",
        chunksize: int | None = None,
    ) -> None:
        try:
            filtered_df = self._filter_dataframe(df, table_name)
            if filtered_df.empty:
                logger.warning(
                    f"No matching columns found in DataFrame for table '{table_name}'. Insert skipped."
                )
                return

            filtered_df.to_sql(
                table_name,
                self.engine,
                schema=self.schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                dtype=self._infer_dtypes(filtered_df),
            )
            logger.info(f"Successfully inserted DataFrame into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error inserting DataFrame into table '{table_name}': {e}")

    def insert_record(self, table_name: str, record: dict) -> None:
        try:
            df = pd.DataFrame([record])
            self.insert_dataframe(df, table_name)
        except Exception as e:
            logger.error(f"Error inserting record into table '{table_name}': {e}")

    def update_record(self, table_name: str, update_values: dict, conditions: dict) -> None:
        try:
            set_clause = ", ".join([f"{col} = :set_{col}" for col in update_values.keys()])
            where_clause = " AND ".join(
                [f"{col} = :where_{col}" for col in conditions.keys()]
            )
            params = {
                **{f"set_{col}": val for col, val in update_values.items()},
                **{f"where_{col}": val for col, val in conditions.items()},
            }
            self.execute(
                f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};",
                params=params,
            )
            logger.info(f"Successfully updated records in '{table_name}'.")
        except Exception as e:
            logger.error(f"Error updating records in table '{table_name}': {e}")

    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: list[str],
        update_columns: list[str] | None = None,
        chunksize: int | None = None,
    ) -> None:
        try:
            filtered_df = self._filter_dataframe(df, table_name)
            if filtered_df.empty:
                logger.warning(
                    f"No matching columns found in DataFrame for table '{table_name}'. Upsert skipped."
                )
                return

            temp_table_name = f"temp_upsert_{table_name}_{uuid.uuid4().hex[:8]}"
            insert_columns = filtered_df.columns.tolist()
            conflict_clause = ", ".join(conflict_columns)
            update_columns = update_columns or [
                column for column in insert_columns if column not in conflict_columns
            ]
            insert_clause = ", ".join(insert_columns)
            select_clause = ", ".join(insert_columns)

            if update_columns:
                update_clause = ", ".join(
                    [f"{column} = EXCLUDED.{column}" for column in update_columns]
                )
                upsert_sql = f"""
                    INSERT INTO {self.schema}.{table_name} ({insert_clause})
                    SELECT {select_clause} FROM {temp_table_name}
                    ON CONFLICT ({conflict_clause}) DO UPDATE
                    SET {update_clause};
                """
            else:
                upsert_sql = f"""
                    INSERT INTO {self.schema}.{table_name} ({insert_clause})
                    SELECT {select_clause} FROM {temp_table_name}
                    ON CONFLICT ({conflict_clause}) DO NOTHING;
                """

            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        CREATE TEMP TABLE {temp_table_name}
                        AS SELECT * FROM {self.schema}.{table_name} WITH NO DATA;
                        """
                    )
                )
                filtered_df.to_sql(
                    temp_table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    dtype=self._infer_dtypes(filtered_df),
                )
                conn.execute(text(upsert_sql))

            logger.info(f"Successfully upserted DataFrame into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error upserting DataFrame into table '{table_name}': {e}")


if __name__ == "__main__":
    db = connector(schema="ods")
    df = db.query_dataframe("SELECT * FROM information_schema.tables LIMIT 10;")
    print(df.head())
