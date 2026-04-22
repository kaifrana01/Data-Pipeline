import os
import requests
import pandas as pd
import re
from urllib.parse import urlparse
from PyPDF2 import PdfReader
from docx import Document
from sqlalchemy import create_engine

# mssql conn
def get_mssql_engine():
    server = '127.0.0.1'   # change if needed
    database = 'pipeline_Database'
    username = 'sa'
    password = 'Rana@123'
    driver = 'ODBC+Driver+17+for+SQL+Server'
    from sqlalchemy import create_engine

def get_mssql_engine():
    conn_str = (
        "mssql+pyodbc://sa:Rana%40123@localhost/pipeline_Database"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(conn_str)

# local file
def get_file(path):
    path = path.strip().strip('"')

    if path.startswith("http://") or path.startswith("https://"):
        try:
            response = requests.get(path)
            response.raise_for_status()
            filename = os.path.basename(urlparse(path).path) or "downloaded_file"
            with open(filename, "wb") as f:
                f.write(response.content)
            print(f"Downloaded file: {filename}")
            return filename
        except Exception as e:
            print("Error downloading file:", e)
            return None
    else:
        if os.path.exists(path):
            print(f"Using local file: {path}")
            return path
        else:
            print("File not found!")
            return None

# Extract information
def extract_info(text):
    data = {}

    # Email
    email = re.findall(r"[\w\.-]+@[\w\.-]+", text)
    data["email"] = email[0] if email else None

    # Phone
    phone = re.findall(r"\+?\d[\d\s\-]{8,}", text)
    data["phone"] = phone[0] if phone else None

    # Name 
    lines = text.strip().split("\n")
    data["name"] = lines[0] if lines else None

    return data

# read
def read_pdf(file):
    reader = PdfReader(file)
    text = "".join([page.extract_text() or "" for page in reader.pages])
    return extract_info(text)

def read_word(file):
    doc = Document(file)
    text = "\n".join([p.text for p in doc.paragraphs])
    return extract_info(text)

def read_excel_csv(file):
    df = pd.read_csv(file) if file.endswith(".csv") else pd.read_excel(file)
    return df.to_dict(orient="records")

def read_database(file):
    engine = create_engine(f"sqlite:///{file}")
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", engine)
    print("\nTables:\n", tables)
    for table in tables['name']:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"\nTable: {table}")
        print(df.head())

# save to smsql
def save_to_mssql(data):
    engine = get_mssql_engine()

    if isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = pd.DataFrame(data)

    df.to_sql("extracted_data", engine, if_exists="append", index=False)
    print("Data saved to MSSQL")

# main process
def process_file(path):
    file = get_file(path)
    if not file:
        return

    if file.endswith(".pdf"):
        data = read_pdf(file)
    elif file.endswith(".docx"):
        data = read_word(file)
    elif file.endswith((".csv", ".xls", ".xlsx")):
        data = read_excel_csv(file)
    elif file.endswith(".db"):
        read_database(file)
        return
    else:
        print("Unsupported file type")
        return

    print("\nExtracted Data:\n", data)

    # Save to database
    save_to_mssql(data)


if __name__ == "__main__":
    url = input("Enter file path: ")
    process_file(url)