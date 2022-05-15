import sqlalchemy as sa
from sqlalchemy.schema import FetchedValue

metadata = sa.MetaData(schema="products")

stg_products = sa.Table(
    "stg_products",
    metadata,
    sa.Column("sku", sa.VARCHAR(64)),
    sa.Column("name", sa.VARCHAR(64)),
    sa.Column("description", sa.Text),
)

dwh_skus = sa.Table(
    "skus",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("sku", sa.VARCHAR(64), nullable=False, unique=True),
)

dwh_names = sa.Table(
    "names",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.VARCHAR(64), nullable=False, unique=True),
)

dwh_products = sa.Table(
    "products",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("sku_id", sa.Integer, sa.ForeignKey("skus.id"), nullable=False),
    sa.Column("name_id", sa.Integer, sa.ForeignKey("names.id"), nullable=False),
    sa.Column("description", sa.Text),
    sa.Column("created_at", sa.TIMESTAMP, server_default=FetchedValue()),
    sa.Column("updated_at", sa.TIMESTAMP, server_onupdate=FetchedValue()),
    sa.UniqueConstraint("sku_id", "name_id", name="uk_sku_id__name_id"),
)
