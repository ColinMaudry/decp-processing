from datetime import datetime
from dotenv import load_dotenv
import json
import os
import shutil

if not os.path.exists(".env"):
    print("Création du fichier .env à partir de template.env")
    shutil.copyfile("template.env", ".env")

load_dotenv()

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[2:10]

DIST_DIR = f"dist/" + DATE_NOW
DECP_PROCESSING_PUBLISH = os.getenv("DECP_PROCESSING_PUBLISH")

SIRENE_DATA_DIR = os.getenv("SIRENE_DATA_DIR")

with open(os.environ["DECP_JSON_FILES_PATH"]) as f:
    DECP_JSON_FILES = json.load(f)
