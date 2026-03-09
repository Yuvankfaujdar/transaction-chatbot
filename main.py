from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import psycopg2
import os
import json
from openai import OpenAI

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ── Load schema once at startup ──────────────────────────────────────────────
with open("transaction_schema_v2.json", "r") as f:
    DB_SCHEMA = json.load(f)

def build_schema_prompt(schema: dict) -> str:
    table = schema["tables"][0]
    lines = []
    lines.append(f"DATABASE DESCRIPTION:\n{schema['database_description']}\n")
    lines.append("CRITICAL QUERY RULES:")
    for key, rule in schema["default_filters"].items():
        lines.append(f"  - {rule}")
    lines.append("")
    lines.append(f"TABLE: {table['name']}")
    lines.append(f"{table['description']}\n")
    lines.append("COLUMNS:")
    for col in table["columns"]:
        desc = col["description"]
        if "values" in col:
            if isinstance(col["values"], dict):
                vals = "; ".join([f"{k}: {v}" for k, v in col["values"].items()])
            else:
                vals = ", ".join(col["values"])
            desc += f" Possible values: {vals}"
        if "example" in col:
            desc += f" Example: {col['example']}"
        lines.append(f"  - {col['name']} ({col['type']}): {desc}")
    lines.append("")
    lines.append("EXAMPLE QUESTION → SQL PATTERNS (follow these closely):")
    for i, qa in enumerate(table["common_questions"], 1):
        lines.append(f"  {i}. Q: {qa['question']}")
        lines.append(f"     SQL: {qa['sql_pattern']}")
    lines.append("")
    return "\n".join(lines)

SCHEMA_PROMPT = build_schema_prompt(DB_SCHEMA)

# ── Database helpers ─────────────────────────────────────────────────────────
def get_user_transactions(user_id):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sender_name, transaction_amount, transaction_currency,
               posting_amount, posting_currency, transaction_type, created_at
        FROM transactions
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 50
    """, (user_id,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def run_sql(query: str):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    conn.close()
    return col_names, rows

# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/")
def home():
    return {"message": "Transaction chatbot backend running"}

@app.get("/users")
def get_users():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT ON (user_id) user_id, full_name, customer_reference
        FROM transactions
        ORDER BY user_id, full_name ASC
    """)
    rows = cursor.fetchall()
    conn.close()
    return {
        "users": [
            {
                "user_id": row[0],
                "full_name": row[1],
                "customer_reference": row[2]
            }
            for row in rows
        ]
    }

@app.get("/transactions/{user_id}")
def transactions(user_id: str):
    data = get_user_transactions(user_id)
    return {"user_id": user_id, "transactions": data}

@app.post("/chat")
def chat(payload: dict):
    user_id = payload["user_id"]
    question = payload["question"]

    # ── Step 1: Generate SQL ─────────────────────────────────────────────────
    sql_system_prompt = f"""
You are a PostgreSQL data analyst working with a financial transactions database.

{SCHEMA_PROMPT}

Your job is to write a single, valid PostgreSQL query to answer the user's question.

Rules:
  - Always filter by user_id = '{user_id}'
  - Use only the transactions table
  - Never SUM posting_amount across multiple transaction_types in the same query
  - Use INVOICED_PAYOUT for received amount questions (posting_amount = INR received)
  - Use INVOICED_PAYMENT for sender currency questions (transaction_currency, transaction_amount)
  - Use PAYOUT_FEE for fee questions (posting_amount = INR fee)
  - Use DATE_TRUNC for time grouping
  - Return ONLY the raw SQL query, no explanation, no markdown, no code fences
"""

    sql_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": sql_system_prompt},
            {"role": "user", "content": question}
        ]
    )

    sql_query = sql_response.choices[0].message.content.strip()
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    # ── Step 2: Execute SQL ──────────────────────────────────────────────────
    try:
        col_names, result = run_sql(sql_query)
    except Exception as e:
        return {
            "answer": f"I wasn't able to run that query. Error: {str(e)}",
            "sql_used": sql_query,
            "error": True
        }

    # ── Step 3: Natural language answer ─────────────────────────────────────
    answer_prompt = f"""
The user asked: "{question}"

The SQL query returned these columns: {col_names}
With these results: {result}

Explain the result clearly and concisely in plain English.
If amounts are in INR, mention that.
If the result is empty, say no transactions were found.
Do not mention SQL or technical details.
"""

    answer_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": answer_prompt}]
    )

    return {
        "answer": answer_response.choices[0].message.content,
        "sql_used": sql_query,
        "columns": col_names,
        "raw_result": result
    }
