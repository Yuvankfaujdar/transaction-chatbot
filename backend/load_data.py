import sqlite3
import pandas as pd

# Load CSV
df = pd.read_csv("transactions.csv")

# Connect to SQLite database
conn = sqlite3.connect("transactions.db")

# Write data into a table called 'transactions'
df.to_sql("transactions", conn, if_exists="replace", index=False)

# Close connection
conn.close()

print("Data successfully loaded into transactions.db")