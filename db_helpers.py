from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import sqlalchemy as sa
import pandas as pd


@dataclass(frozen=True)
class DBCreds:
    username: str
    password: str
    host: str
    port: int

    def create_db_connection(self, engine: str, **extra_kwargs) -> sa.engine.Engine:
        db_uri = f"{engine}://{self.username}:{self.password}@{self.host}:{self.port}"
        return sa.create_engine(db_uri, **extra_kwargs)


def load_csv_to_table(
    *,
    db_creds: DBCreds,
    engine: str,
    table: sa.Table,
    truncate_before_insert: bool = False,
    csv_path: str,
    read_csv_kwargs: Optional[Dict[str, Any]] = None,
    chunksize: int = 1e5,
    log_progress: Optional[Callable[[str], None]] = None,
    engine_kwargs: Optional[Dict[str, Any]] = None,
):
    db_engine = db_creds.create_db_connection(
        engine, **(engine_kwargs if engine_kwargs is not None else {})
    )
    ensure_tables([table], db_engine)

    read_csv_kwargs = read_csv_kwargs if read_csv_kwargs is not None else {}

    with db_engine.begin() as conn:
        if truncate_before_insert:
            truncate_table(table, conn)

        chunk_df: pd.DataFrame
        for chunk_df in pd.read_csv(csv_path, chunksize=chunksize, **read_csv_kwargs):
            chunk_df.to_sql(
                name=table.name,
                schema=table.schema,
                if_exists="append",
                index=False,
                con=conn,
            )
            if log_progress is not None:
                log_progress(
                    f"Bulk inserted pandas dataframe of shape: {chunk_df.shape}"
                )


def truncate_table(table: sa.Table, conn: sa.engine.Connection):
    conn.execute(f"TRUNCATE TABLE {table.schema}.{table.name};")


def ensure_tables(tables: List[sa.Table], db_engine: sa.engine.Engine):
    for table in tables:
        table.metadata.create_all(db_engine, tables=[table], checkfirst=True)
