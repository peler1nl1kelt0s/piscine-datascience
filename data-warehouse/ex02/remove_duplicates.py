import pandas as pd
from sqlalchemy import MetaData
import sqlalchemy as db
import os


def if_table_exists(engine, table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    if table_name in metadata.tables:
        print(f"Table {table_name} already exist")
    return table_name in metadata.tables


def to_postgre(data : pd.DataFrame, table : str):
    engine = db.create_engine('postgresql://museker:123@localhost:5432/piscineds')
    data.to_sql(
        name=table,
        con=engine,
        index=False,
        if_exists="replace",
        dtype = {
            "event_time": db.DateTime(),
            "event_type": db.types.String(length=255),
            "product_id": db.types.Integer(),
            "price": db.types.Float(),
            "user_id": db.types.BigInteger(),
            "user_session": db.types.UUID(as_uuid=True)
        }
    )
    print(f"{table} sended successfully!")

def get_data(csv_name : str) -> pd.DataFrame:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "..", "..","..","subject", "customer", csv_name)
    csv_path = os.path.abspath(csv_path)
    data = pd.read_csv(csv_path)
    return data


def get_and_push():
    name_list = ["data_2022_dec", "data_2022_nov", "data_2022_oct", "data_2023_jan", "data_2023_jan"]
    customers = pd.concat([get_data(f + ".csv") for f in name_list])
    customers = customers.drop_duplicates(subset=["event_time", "event_type", "product_id", "price", "user_id", "user_session"])
    to_postgre(data=customers, table="customers")
    customers = pd.DataFrame()


def main():
    get_and_push()

if __name__ == "__main__":
    main()
