import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import sqlalchemy as db

def to_postgre(data : pd.DataFrame, tableName : str):
    engine = db.create_engine('postgresql://museker:123@localhost:5432/piscineds')
    data.to_sql(
        name=tableName,
        con=engine,
        dtype = {
            "event_time": db.DateTime(),
            "event_type": db.types.String(length=255),
            "product_id": db.types.Integer(),
            "price": db.types.Float(),
            "user_id": db.types.BigInteger(),
            "user_session": db.types.UUID(as_uuid=True)
        }
    )
    print("the data sended successfully!")


def get_data() -> pd.DataFrame:
    data = pd.read_csv("data_2022_oct.csv")
    return data

def main():
    data = get_data()
    to_postgre(data, "data_2022_oct")

if __name__ == "__main__":
    main()
