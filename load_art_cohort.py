import pandas as pd, pyodbc, os

SERVER   = os.getenv("SQLSERVER_HOST", "localhost")   
PORT     = os.getenv("SQLSERVER_PORT", "1433")
DB       = os.getenv("SQLSERVER_DB",   "carepro")
USER     = os.getenv("SQLSERVER_USER", "airflow_user")
PWD      = os.getenv("SQLSERVER_PASSWORD", "Tr@cK3RIHM_!2025")
TABLE    = os.getenv("SQL_TABLE", "dbo.art_cohort")
CSV      = r"C:\data\art_cohort.csv"

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER},{PORT};DATABASE={DB};UID={USER};PWD={PWD};"
    f"Encrypt=yes;TrustServerCertificate=yes",
    autocommit=True
)

df = pd.read_csv(CSV, dtype=str, keep_default_na=False, na_values=["", "NULL"], encoding="utf-8")

# ensure table exists as NVARCHAR(MAX) everywhere
cols = ",\n".join([f"[{c}] NVARCHAR(MAX) NULL" for c in df.columns])
ddl  = f"IF OBJECT_ID('{TABLE}','U') IS NULL CREATE TABLE {TABLE} ( {cols} );"
with conn.cursor() as cur:
    cur.execute(ddl)

# fast executemany insert
placeholders = ",".join(["?"] * len(df.columns))
cols_quoted  = ",".join([f"[{c}]" for c in df.columns])
insert_sql   = f"INSERT INTO {TABLE} ({cols_quoted}) VALUES ({placeholders})"

# chunk to keep memory stable
rows = 0
with conn.cursor() as cur:
    cur.fast_executemany = True
    for start in range(0, len(df), 10000):
        chunk = df.iloc[start:start+10000].where(pd.notnull(df), None).values.tolist()
        cur.executemany(insert_sql, chunk)
        rows += len(chunk)
print(f"Loaded {rows} rows into {TABLE}")
