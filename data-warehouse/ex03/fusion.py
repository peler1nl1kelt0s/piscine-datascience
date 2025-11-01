import pandas as pd
import sqlalchemy as db
import os

def update_customers_direct():
    engine = db.create_engine('postgresql://museker:mysecretpassword@localhost:5432/piscineds')
    
    customers = pd.read_sql('SELECT * FROM customers', con=engine)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.abspath(os.path.join(base_dir, '..', 'subject', 'item', 'item.csv'))
    item_df = pd.read_csv(csv_path)
    
    merged_df = pd.merge(customers, item_df, on="product_id", how="left")
    
    merged_df.to_sql(
        name='customers',
        con=engine,
        index=False,
        if_exists='replace'
    )
    
    print("customers tablosu başarıyla güncellendi (doğrudan)!")

if __name__ == "__main__":
    update_customers_direct()
