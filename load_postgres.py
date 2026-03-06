import pandas as pd
import psycopg2
import os

df = pd.read_csv("transactions.csv")

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
user_id INTEGER,
payer TEXT,
amount FLOAT,
currency TEXT,
created_at TIMESTAMP
)
""")

for _,row in df.iterrows():

    cursor.execute("""
    INSERT INTO transactions VALUES (%s,%s,%s,%s,%s)
    """,(row.user_id,row.payer,row.amount,row.currency,row.created_at))

conn.commit()
conn.close()
