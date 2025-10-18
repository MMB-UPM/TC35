import pandas as pd
import sys

if len(sys.argv) != 3:
    print("Uso: python mostrar_filas_csv.py <ruta_csv_procesado> <numero_filas>")
    sys.exit(1)

ruta_csv_procesado = sys.argv[1]

try:
    X = int(sys.argv[2])  # El segundo argumento es el número de filas
except ValueError:
    print("Por favor, introduce un número válido para el número de filas.")
    sys.exit(1)

if X <= 0:
    print("Por favor, introduce un número positivo para el número de filas.")
    sys.exit(1)

df = pd.read_csv(ruta_csv_procesado)
print(df.head(X))

