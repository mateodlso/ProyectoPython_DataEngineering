# banks_project.py  (colócalo en .../largest-banks-etl/)
# -------------------------------------------------------
# ETL – Top‑10 bancos por capitalización de mercado
# -------------------------------------------------------

import sqlite3, datetime, requests, pandas as pd, numpy as np
from bs4 import BeautifulSoup
from pathlib import Path

# ----------  RUTAS Y CONSTANTES  -----------------------
BASE_DIR = Path(__file__).resolve().parent        # carpeta donde está este .py

URL_WIKI = (
    "https://web.archive.org/web/20230908091635/"
    "https://en.wikipedia.org/wiki/List_of_largest_banks"
)
CSV_URL  = ("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/"
            "IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv")

CSV_XRATE_PATH   = BASE_DIR / "exchange_rate.csv"
OUTPUT_CSV_PATH  = BASE_DIR / "Largest_banks_data.csv"
DB_PATH          = BASE_DIR / "Banks.db"
TABLE_NAME       = "Largest_banks"
LOG_FILE         = BASE_DIR / "code_log.txt"

# ----------  FUNCIÓN DE LOG  ---------------------------
def log_progress(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{ts} : {msg}\n")

# ----------  DESCARGAR CSV SI NO EXISTE  ---------------
def ensure_exchange_csv():
    if CSV_XRATE_PATH.exists() and CSV_XRATE_PATH.stat().st_size > 0:
        return
    print("▶ Descargando exchange_rate.csv ...")
    r = requests.get(CSV_URL, timeout=30)
    r.raise_for_status()
    CSV_XRATE_PATH.write_bytes(r.content)

# ----------  EXTRACT  ----------------------------------
def extract() -> pd.DataFrame:
    tables = pd.read_html(URL_WIKI, attrs={"class": "wikitable"}, flavor="bs4")
    for tbl in tables:
        if any("Market cap" in str(c) for c in tbl.columns):
            df = tbl.iloc[:10, :3].copy()
            df.columns = ["Rank", "Name", "MC_USD_Billion"]
            df["MC_USD_Billion"] = (
                df["MC_USD_Billion"]
                .astype(str)
                .str.replace(r"[^\d.]", "", regex=True)
                .astype(float)
            )
            return df.drop(columns="Rank")
    raise ValueError("No se encontró la tabla requerida en la página.")

# ----------  TRANSFORM  --------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    xdf = pd.read_csv(CSV_XRATE_PATH)   # columnas: Currency,Rate
    xrate = xdf.set_index("Currency")["Rate"].astype(float).to_dict()

    df["MC_GBP_Billion"] = np.round(df["MC_USD_Billion"] * xrate["GBP"], 2)
    df["MC_EUR_Billion"] = np.round(df["MC_USD_Billion"] * xrate["EUR"], 2)
    df["MC_INR_Billion"] = np.round(df["MC_USD_Billion"] * xrate["INR"], 2)
    return df

# ----------  LOADS  ------------------------------------
def load_to_csv(df: pd.DataFrame):
    df.to_csv(OUTPUT_CSV_PATH, index=False)

def load_to_db(df: pd.DataFrame, conn: sqlite3.Connection):
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

def run_query(q: str, conn: sqlite3.Connection):
    print(f"\n➜ {q}")
    print(pd.read_sql(q, conn))

# ----------  MAIN  -------------------------------------
if __name__ == "__main__":
    log_progress("Preliminares completos. Iniciando ETL")

    ensure_exchange_csv()  # garantiza que el CSV esté presente

    df = extract()
    log_progress("Extracción completada. Iniciando transformación")

    df = transform(df)
    log_progress("Transformación completada. Iniciando carga")

    load_to_csv(df)
    log_progress("Datos guardados en CSV")

    conn = sqlite3.connect(DB_PATH)
    log_progress("Conexión SQL iniciada")

    load_to_db(df, conn)
    log_progress("Datos cargados en la base de datos, ejecutando consultas")

    for q in [
        "SELECT * FROM Largest_banks",
        "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
        "SELECT Name FROM Largest_banks LIMIT 5",
    ]:
        run_query(q, conn)
    
    print("\nTabla transformada (Task 3b):")
    print(df.to_string(index=False))

    log_progress("Proceso completo")
    conn.close()
    log_progress("Conexión SQL cerrada")
