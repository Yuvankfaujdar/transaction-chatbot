import pandas as pd
import psycopg2

DATABASE_URL = "postgresql://postgres:uoUtsOtifvCDUjUOordSOAEUJwpCGsXF@interchange.proxy.rlwy.net:23643/railway"

df = pd.read_csv("transactions.csv")

# Convert date format
df["created_at"] = pd.to_datetime(df["created_at"], format="%d-%m-%Y %H:%M")

conn = psycopg2.connect(DATABASE_URL)
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

for _, row in df.iterrows():

    cursor.execute("""
    INSERT INTO transactions VALUES (%s,%s,%s,%s,%s)
    """,(
        row["user_id"],
        row["payer"],
        row["amount"],
        row["currency"],
        row["created_at"]
    ))

conn.commit()

cursor.close()
conn.close()

print("Data successfully loaded into Postgres")
