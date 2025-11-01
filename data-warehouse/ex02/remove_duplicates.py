import pandas as pd
from sqlalchemy import MetaData
import sqlalchemy as db
import os


def remove_duplicates():
    query = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remove_duplicates.sql")
    query = os.path.abspath(query)
    with open(query, 'r') as file:
        sql_query = file.read()
    engine = db.create_engine('postgresql://museker:mysecretpassword@localhost:5432/piscineds')
    with engine.connect() as connection:
        connection.execute(db.text(sql_query))
        connection.commit()
    print("Duplicates removed successfully!")


def main():
    remove_duplicates()

if __name__ == "__main__":
    main()
