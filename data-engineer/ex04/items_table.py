import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import sqlalchemy as db
import os


def if_table_exists(engine, table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    if table_name in metadata.tables:
        print(f"Table {table_name} already exist")
    return table_name in metadata.tables


def to_postgre(data : pd.DataFrame, tableName : str):
    engine = db.create_engine('postgresql://museker:123@localhost:5432/piscineds')
    if not if_table_exists(engine=engine, table_name=tableName):
        data.to_sql(
            name=tableName,
            con=engine,
            index=False,
            if_exists="append",
            dtype = {
                "product_id": db.types.Integer(),
                "category_id": db.types.BigInteger(),
                "category_code": db.types.String(length=255),
                "brand": db.types.String(length=255),
            }
        )
        print(f"{tableName} sended successfully!")



def get_data(csv_name : str) -> pd.DataFrame:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "..", "..","..","subject", "item", csv_name)
    csv_path = os.path.abspath(csv_path)
    data = pd.read_csv(csv_path)
    return data


def get_and_push():
    name_list = ["item"]
    for i in name_list:
        data = get_data(i + ".csv")
        to_postgre(data=data, tableName=i)
        data = pd.DataFrame()


def main():
    get_and_push()


if __name__ == "__main__":
    main()
