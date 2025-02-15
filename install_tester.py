# Python test
print("Hello world!")
import sys, os
""""
print(f"Python version: {sys.version}")
print(f"Python path:    {sys.path}")
print("")
"""

# PyTorch test
import torch
print(f"PyTorch version: {torch.__version__}")
print(f"CUDA available:  {torch.cuda.is_available()}")

import time
n = int(2**10)
# CPU test
device = torch.device("cpu")
start = time.time()
matrix1 = torch.rand(n, n, device=device)
matrix2 = torch.rand(n, n, device=device)
result12 = matrix1.matmul(matrix2)
cpu_time = time.time() - start

# GPU test
device = torch.device("cuda")
start = time.time()
matrix3 = torch.rand(n, n, device=device)
matrix4 = torch.rand(n, n, device=device)
result34 = matrix3.matmul(matrix4)
gpu_time = time.time() - start

print(f"CPU time: {cpu_time:.2f} seconds")
print(f"GPU time: {gpu_time:.2f} seconds")
print(f"Speedup: {cpu_time / gpu_time:.2f}x")

# Postgres test
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

# Connect to the database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)

cur = conn.cursor()

# Create a table
cur.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        price DECIMAL(10, 2) NOT NULL
    )
""")

# Insert a row
cur.execute("INSERT INTO stocks (symbol, price) VALUES (%s, %s)", ('AAPL', 175.50))

# Fetch all rows
cur.execute("SELECT * FROM stocks")
rows = cur.fetchall()
for row in rows:
    print(row)

# Drop the table
cur.execute("DROP TABLE stocks")

# Commit changes and close
conn.commit()
cur.close()
conn.close()