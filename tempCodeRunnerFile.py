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
    server = '127.0.0.1'   # change