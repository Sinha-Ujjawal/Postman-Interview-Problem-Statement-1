import toml
from prefect import Flow
from prefect.executors import LocalDaskExecutor

from flow import create_flow, FlowParameters
from db_helpers import DBCreds


def db_creds_from_toml(toml_file: str) -> DBCreds:
    toml_contents = toml.load(toml_file)
    return DBCreds(
        username=toml_contents["USERNAME"],
        password=toml_contents["PASSWORD"],
        host=toml_contents["HOST"],
        port=toml_contents["PORT"],
    )


def create_flow_for_main() -> Flow:
    db_creds = db_creds_from_toml("./db.toml")

    flow_params = FlowParameters(
        db_creds=db_creds,
        products_csv_path="./data/products.csv",
        products_csv_chunksize=1_000_000,
        verbose=True,
    )

    return create_flow(flow_params=flow_params, flow_name="Products")


def main():
    flow = create_flow_for_main()
    _state = flow.run(executor=LocalDaskExecutor())


if __name__ == "__main__":
    main()
