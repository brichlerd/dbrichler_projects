""" """

import os
from datetime import UTC, datetime
from typing import Sequence, Any

import pandas as pd
import psycopg
from psycopg import sql

from util import get_logger


class Database:
    """Simple PostgreSQL utility class using psycopg v3."""

    def __init__(
        self,
        dbname: str | None,
        user: str | None,
        password: str | None,
        host: str | None,
        port: int = 5432,
    ):
        """Initialize database connection parameters and connect."""
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = self.connect()

    @property
    def current_timestamp(self) -> datetime:
        """Return the current UTC time."""
        return datetime.now(UTC)

    def connect(self) -> psycopg.Connection:
        """Establish and return a PostgreSQL database connection."""
        return psycopg.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def execute_sql(self, query: sql.Composed, query_params: tuple = ()) -> None:
        """Execute a SQL command with optional parameters."""
        with self.conn.cursor() as cur:
            try:
                cur.execute(query, query_params)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                raise RuntimeError(f"Failed to execute SQL: {e}") from e

    def fetch_data(self, query: sql.Composed | sql.SQL, query_params: tuple = ()) -> pd.DataFrame:
        """Execute a query and return the result as a pandas DataFrame."""
        with self.conn.cursor() as cur:
            cur.execute(query, query_params)
            if cur.description is None:
                raise RuntimeError("Cursor description is None — no columns returned")
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
        return pd.DataFrame(data, columns=columns)

    # Safely prepare data
    @staticmethod
    def safe_value(val):
        if isinstance(val, list):
            return val
        if pd.isna(val):
            return None
        return val

    def bulk_insert(self, table_name: str, df: pd.DataFrame, platform: str = "Leaflink") -> None:
        """Insert bulk data from a DataFrame into the specified table."""

        if df.empty:
            return

        df.loc[:, "platform_name"] = platform
        data = [
            tuple(self.safe_value(val) for val in row)
            for row in df.itertuples(index=False, name=None)
        ]

        table_identifier = sql.Identifier(table_name)
        columns = [sql.Identifier(col) for col in df.columns]
        columns_sql = sql.SQL(", ").join(columns)

        placeholders = sql.SQL(", ").join(sql.Placeholder() * len(df.columns))
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            table_identifier, columns_sql, placeholders
        )

        try:
            with self.conn.cursor() as cur:
                cur.executemany(insert_sql, data)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Bulk insert failed: {e}") from e

    def delete_rows_by_keys(
        self,
        table_name: str,
        df: pd.DataFrame,
        key_cols: Sequence[str],
        extra_filters: dict[str, Any] | None = None,
        chunk_size: int = 1000,
    ) -> int:
        """
        Delete rows from `table_name` where the composite key `key_cols`
        matches any row in `df[key_cols]`.

        Returns total rows deleted.
        deleted = db.delete_rows_by_keys(
            table_name="TABLE",
            df=my_df,                  # must contain both columns below
            key_cols=["order_id", "product_id"],
            extra_filters={"seller_id": 17875},  # optional filter on the target table
        )
        """
        # 0) Fast exits / validation
        if df.empty:
            return 0

        missing = [c for c in key_cols if c not in df.columns]
        if missing:
            raise ValueError(f"DataFrame missing key columns: {missing}")

        if "order_number" in key_cols:
            df["order_number"] = df["order_number"].astype(str)

        # 1) Deduplicate keys to avoid bloating the VALUES list
        key_df = df[list(key_cols)].drop_duplicates(ignore_index=True)

        total_deleted = 0

        # 2) Prepare identifiers once
        tbl_ident = sql.Identifier(table_name)
        cols_ident = [sql.Identifier(c) for c in key_cols]

        # (t.col1, t.col2, ...) and (v.col1, v.col2, ...)
        t_cols = sql.SQL(", ").join(
            sql.Composed([sql.Identifier("t"), sql.SQL("."), c]) for c in cols_ident
        )
        v_cols = sql.SQL(", ").join(
            sql.Composed([sql.Identifier("v"), sql.SQL("."), c]) for c in cols_ident
        )

        # 3) Optional extra filters (applied to target table `t`)
        extra_clause: sql.Composable = sql.SQL("")
        params_extra: list[Any] = []
        if extra_filters:
            pieces = []
            for k, v in extra_filters.items():
                pieces.append(
                    sql.Composed(
                        [sql.Identifier("t"), sql.SQL("."), sql.Identifier(k), sql.SQL(" = %s")]
                    )
                )
                params_extra.append(v)
            if pieces:
                extra_clause = sql.SQL(" AND ") + sql.SQL(" AND ").join(pieces)

        # 4) Chunked delete
        # Build the VALUES row placeholder for one row: "(%s, %s, ...)"
        one_row_ph = "(" + ", ".join(["%s"] * len(key_cols)) + ")"

        with self.conn.cursor() as cur:
            for start in range(0, len(key_df), chunk_size):
                chunk = key_df.iloc[start : start + chunk_size]
                rows = [tuple(chunk.iloc[i][list(key_cols)].tolist()) for i in range(len(chunk))]
                # Flatten parameters for VALUES
                vals_flat: list[Any] = [v for row in rows for v in row]

                # "(%s,%s),(%s,%s),..."
                values_block = sql.SQL(", ").join(sql.SQL(one_row_ph) for _ in rows)

                delete_sql = (
                    sql.SQL("DELETE FROM {} AS t USING (VALUES ").format(tbl_ident)
                    + values_block
                    + sql.SQL(") AS v (")
                    + sql.SQL(", ").join(cols_ident)
                    + sql.SQL(") WHERE (")
                    + t_cols
                    + sql.SQL(") = (")
                    + v_cols
                    + sql.SQL(")")
                    + extra_clause
                )

                cur.execute(delete_sql, vals_flat + params_extra)
                total_deleted += cur.rowcount

            self.conn.commit()

        return total_deleted


def __connect_db__(dbname: str, user: str, password: str, host: str, port: int = 5432) -> Database:
    """Establish a database connection using environment variables."""
    logger = get_logger()
    db = Database(dbname=dbname, user=user, password=password, host=host, port=port)
    try:
        with db.conn.cursor() as cur:
            cur.execute("SELECT 1;")
        logger.info("Connected to %s Database", dbname)
    except psycopg.DatabaseError as db_error:
        db.conn.rollback()
        logger.exception("%s Error: %s", dbname, db_error)
        raise RuntimeError(str(db_error)) from db_error
    return db


def connect_weather() -> Database:
    """Marketplace analytics database connection"""
    user = os.getenv("APP_USER", "")
    password = os.getenv("weather123", "")
    host = os.getenv("DB_HOST", "")
    port = int(os.getenv("DB_PORT", 5432))

    return __connect_db__("weather_data", user, password, host, port)
