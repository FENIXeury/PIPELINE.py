import pandas as pd
import pyodbc
from datetime import datetime

print("\nIniciando el Pipeline ETL\n")

# ===============================================
# Conexión a SQL Server 
# ===============================================
conn = pyodbc.connect(
    "DRIVER={SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=master;"
    "Trusted_Connection=True;"
)
cursor = conn.cursor()

# ===============================================
# Función para limpiar fechas
# ===============================================
def parse_date(x):
    """Intenta interpretar la fecha aunque venga rara."""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(x), fmt).date()
        except:
            continue
    print(f"Advertencia: Fecha '{x}' no se pudo interpretar. Se descarta.")
    return None

# ===============================================
# Cargar CSVs
# ===============================================
products = pd.read_csv("products.csv")
clients = pd.read_csv("clients.csv")
sales = pd.read_csv("sales.csv")

print("CSVs cargados correctamente.")

# ===============================================
# Limpieza de Productos
# ===============================================
products.drop_duplicates(subset=["product_id"], inplace=True)
products["price"] = pd.to_numeric(products["price"], errors="coerce")

avg_price = products["price"].mean()
products["price"].fillna(avg_price, inplace=True)

print("Productos limpiados y validados.")

# ===============================================
# Limpieza de Clientes
# ===============================================
clients.drop_duplicates(subset=["client_id"], inplace=True)

clients["email"] = clients["email"].fillna("sin-email@colmado.do")

clients["country"] = clients.get("country", "República Dominicana")
clients["region"] = clients.get("region", "Santo Domingo")

print("Clientes limpios y listos.")

# ===============================================
# Limpieza de Ventas
# ===============================================
sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce")
sales["price"] = pd.to_numeric(sales["price"], errors="coerce")
sales["date"] = sales["date"].apply(parse_date)

before_len = len(sales)
sales.dropna(subset=["quantity", "date"], inplace=True)

after_len = len(sales)
dropped = before_len - after_len

if dropped > 0:
    print(f"Se eliminaron {dropped} ventas por fecha o cantidad inválida.")

sales["total"] = sales["quantity"] * sales["price"]

print("Ventas limpiadas.")

# ===============================================
# Dimensión Fecha
# ===============================================
dates = pd.DataFrame({"full_date": sales["date"].dropna().unique()})
dates["full_date"] = pd.to_datetime(dates["full_date"])
dates["date_id"] = dates["full_date"].dt.strftime("%Y%m%d").astype(int)
dates["day"] = dates["full_date"].dt.day
dates["month"] = dates["full_date"].dt.month
dates["month_name"] = dates["full_date"].dt.month_name()
dates["quarter"] = dates["full_date"].dt.quarter
dates["year"] = dates["full_date"].dt.year

print("Cargando Dimensión Fecha...")

for _, row in dates.iterrows():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dim_date WHERE date_id = ?)
        INSERT INTO dim_date (date_id, full_date, day, month, month_name, quarter, year)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, row["date_id"], row["date_id"], row["full_date"], row["day"],
       row["month"], row["month_name"], row["quarter"], row["year"])

# ===============================================
# Dimensión Fuente
# ===============================================
print("Cargando Dimensión Fuente...")

for src in sales["source"].dropna().unique():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dim_source WHERE name = ?)
        INSERT INTO dim_source (name, description)
        VALUES (?, ?)
    """, src, src, f"Importado desde {src}")


# ===============================================
# Dimensión Producto
# ===============================================
print("Cargando Dimensión Producto...")

for _, row in products.iterrows():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dim_product WHERE product_id = ?)
        INSERT INTO dim_product (product_id, name, price)
        VALUES (?, ?, ?)
    """, row["product_id"], row["product_id"], row["name"], float(row["price"]))

# ===============================================
# Dimensión Cliente
# ===============================================
print("Cargando Dimensión Cliente...")

for _, row in clients.iterrows():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dim_client WHERE client_id = ?)
        INSERT INTO dim_client (client_id, name, email, country, region)
        VALUES (?, ?, ?, ?, ?)
    """, row["client_id"], row["client_id"], row["name"], row["email"],
       row["country"], row["region"])

# ===============================================
# Tabla de Hechos - Ventas
# ===============================================
print("Cargando Fact Sales...")

for _, row in sales.iterrows():

    date_id = int(pd.to_datetime(row["date"]).strftime("%Y%m%d"))

    cursor.execute("SELECT source_id FROM dim_source WHERE name = ?", row["source"])
    source_id = cursor.fetchone()
    source_id = source_id[0] if source_id else None

    try:
        cursor.execute("""
            INSERT INTO fact_sales
            (sale_id, product_id, client_id, source_id, date_id, quantity, price, total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, row["sale_id"], row["product_id"], row["client_id"],
           source_id, date_id, int(row["quantity"]),
           float(row["price"]), float(row["total"]))
    except Exception as e:
        print(f"Venta {row['sale_id']} no pudo insertarse: {e}")

# ===============================================
# Confirmar cambios
# ===============================================
conn.commit()

print("\nCarga completada con éxito.\n")

# ===============================================
# Resumen
# ===============================================
query_counts = """
SELECT 'dim_source' AS tabla, COUNT(*) AS registros FROM dim_source
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_client', COUNT(*) FROM dim_client
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;
"""
counts = pd.read_sql(query_counts, conn)
print("Cantidad de registros por tabla:")
print(counts)

cursor.close()
conn.close()

print("\nETL Finalizado.\n")

