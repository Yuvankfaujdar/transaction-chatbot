from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import sqlite3
from openai import OpenAI

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add your OpenAI API key here
client = OpenAI(api_key="sk-proj-n0YhtxT1Fl3WP_chWMDiqLVJ7_RaWOcQm7Nvt8YBB9TTz2igopZ0gPZy_vX21Ol99Sczdvczvczxv_qBx8yQT3BlbkFJqpkZhjAEdb9TaYFiRDLYFVHPX0vA0vpN-fAI0BRdvfXyFisBrFLVOIRVLWqEvcWn3FnbrdIT4A")


# Function to fetch transactions for a user
def get_user_transactions(user_id):

    conn = sqlite3.connect("transactions.db")
    cursor = conn.cursor()

    cursor.execute("""
    SELECT payer, amount, currency, created_at
    FROM transactions
    WHERE user_id = ?
    """, (user_id,))

    rows = cursor.fetchall()

    conn.close()

    return rows

def run_sql(query):

    conn = sqlite3.connect("transactions.db")
    cursor = conn.cursor()

    cursor.execute(query)

    rows = cursor.fetchall()

    conn.close()

    return rows

# Home route
@app.get("/")
def home():
    return {"message": "Transaction chatbot backend running"}


# Endpoint to check transactions
@app.get("/transactions/{user_id}")
def transactions(user_id: int):

    data = get_user_transactions(user_id)

    return {
        "user_id": user_id,
        "transactions": data
    }


# Chat endpoint
@app.post("/chat")
def chat(payload: dict):

    user_id = payload["user_id"]
    question = payload["question"]

    # Step 1: Ask GPT to generate SQL
    sql_prompt = f"""
You are a data analyst.

Generate a SQL query to answer the user's question.

Database table name: transactions

Columns:
user_id
payer
amount
currency
created_at

Only generate the SQL query.

User question:
{question}

Filter by user_id = {user_id}
"""

    sql_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":sql_prompt}]
    )

    sql_query = sql_response.choices[0].message.content.strip()

    # Remove markdown code blocks if present

    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    # Step 2: Execute SQL
    result = run_sql(sql_query)

    # Step 3: Convert result to explanation
    answer_prompt = f"""
Explain the result of this query in simple English.

User question:
{question}

SQL result:
{result}
"""

    answer = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":answer_prompt}]
    )

    return {
        "answer": answer.choices[0].message.content,
        "sql_used": sql_query
    }