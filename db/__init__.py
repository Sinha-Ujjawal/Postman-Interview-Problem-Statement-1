from typing import Callable, Optional
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from db_helpers import ensure_tables, DBCreds, load_csv_to_table, truncate_table
from .tables import (
    stg_products,
    dwh_names,
    dwh_products,
    dwh_skus,
    dwh_by_name_no_of_products,
)

MYSQL_ENGINE = "mysql+mysqldb"


def load_csv_to_stg_products(
    *,
    db_creds: DBCreds,
    csv_path: str,
    chunksize: int = 1e5,
    log_progress: Optional[Callable[[str], None]] = None,
):
    load_csv_to_table(
        db_creds=db_creds,
        engine=MYSQL_ENGINE,
        table=stg_products,
        csv_path=csv_path,
        chunksize=chunksize,
        log_progress=log_progress,
        truncate_before_insert=True,
        # read_csv_kwargs={"nrows": 10_000},
    )


def update_skus_table(db_creds: DBCreds, **db_connection_kwargs):
    db_engine = db_creds.create_db_connection(MYSQL_ENGINE, **db_connection_kwargs)
    ensure_tables([stg_products, dwh_skus], db_engine)

    insert_stmt = mysql.insert(dwh_skus).from_select(
        ["sku"], sa.select([sa.func.distinct(stg_products.columns["sku"])])
    )

    do_update_stmt = insert_stmt.on_duplicate_key_update(
        sku=insert_stmt.inserted["sku"],
    )

    with db_engine.begin() as conn:
        conn.execute(do_update_stmt)


def update_names_table(db_creds: DBCreds, **db_connection_kwargs):
    db_engine = db_creds.create_db_connection(MYSQL_ENGINE, **db_connection_kwargs)
    ensure_tables([stg_products, dwh_names], db_engine)

    insert_stmt = mysql.insert(dwh_names).from_select(
        ["name"], sa.select([sa.func.distinct(stg_products.columns["name"])])
    )

    do_update_stmt = insert_stmt.on_duplicate_key_update(
        name=insert_stmt.inserted["name"],
    )

    with db_engine.begin() as conn:
        conn.execute(do_update_stmt)


def update_products_table(db_creds: DBCreds, **db_connection_kwargs):
    db_engine = db_creds.create_db_connection(MYSQL_ENGINE, **db_connection_kwargs)
    ensure_tables([stg_products, dwh_skus, dwh_names, dwh_products], db_engine)

    insert_stmt = mysql.insert(dwh_products).from_select(
        ["sku_id", "name_id", "description", "created_at", "updated_at"],
        (
            sa.select(
                [
                    dwh_skus.columns["id"].label("sku_id"),
                    dwh_names.columns["id"].label("name_id"),
                    stg_products.columns["description"],
                    sa.func.current_timestamp().label("created_at"),
                    sa.func.current_timestamp().label("updated_at"),
                ]
            )
            .select_from(
                stg_products.join(
                    dwh_skus,
                    onclause=stg_products.columns["sku"] == dwh_skus.columns["sku"],
                ).join(
                    dwh_names,
                    onclause=stg_products.columns["name"] == dwh_names.columns["name"],
                )
            )
            .order_by(dwh_skus.columns["id"], dwh_names.columns["id"])
        ),
    )

    do_update_stmt = insert_stmt.on_duplicate_key_update(
        description=insert_stmt.inserted["description"],
        updated_at=insert_stmt.inserted["updated_at"],
    )

    with db_engine.begin() as conn:
        conn.execute(do_update_stmt)


def update_by_name_no_of_products_table(db_creds: DBCreds, **db_connection_kwargs):
    db_engine = db_creds.create_db_connection(MYSQL_ENGINE, **db_connection_kwargs)
    ensure_tables([dwh_products, dwh_by_name_no_of_products], db_engine)

    insert_stmt = sa.insert(dwh_by_name_no_of_products).from_select(
        ["name_id", "no_of_products"],
        sa.select(
            [
                dwh_products.columns["name_id"],
                sa.func.count(sa.distinct(dwh_products.columns["sku_id"])).label(
                    "no_of_products"
                ),
            ]
        ).group_by(dwh_products.columns["name_id"]),
    )

    with db_engine.begin() as conn:
        truncate_table(dwh_by_name_no_of_products, conn)
        conn.execute(insert_stmt)
