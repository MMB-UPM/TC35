import pandas as pd
import sys
import socket

def ip_a_hostname(ip_address):
    try:
        hostname, aliaslist, ipaddrlist = socket.gethostbyaddr(ip_address)
        return hostname
    except (socket.herror, socket.gaierror):
        return ip_address

if len(sys.argv) != 3:
    print("Uso: python script.py <ruta_csv_original> <ruta_csv_base>")
    sys.exit(1)

ruta_csv_original = sys.argv[1]
ruta_csv_base = sys.argv[2]

df = pd.read_csv(ruta_csv_original)
columnas_agrupacion = ['src_ip', 'dst_ip', 'src_port', 'dst_port', 'protocol', 'first']

df['count'] = df.groupby(columnas_agrupacion)['src_ip'].transform('size')
df['pkts_totales'] = df.groupby(columnas_agrupacion)['flow_pkts'].transform('max')
df = df.drop_duplicates(subset=columnas_agrupacion)
df = df.drop(columns=['timestamp', 'flow_pkts'], errors='ignore')
df = df.sort_values(by='count', ascending=False)

ruta_procesado_ip = f"{ruta_csv_base}_procesado_ip.csv"
df.to_csv(ruta_procesado_ip, index=False)
print(f"El archivo CSV con IPs ha sido creado exitosamente en {ruta_procesado_ip}.")

df_nombres = df.copy()

df_nombres['src_ip'] = df_nombres['src_ip'].apply(ip_a_hostname)
df_nombres['dst_ip'] = df_nombres['dst_ip'].apply(ip_a_hostname)

ruta_procesado_nombres = f"{ruta_csv_base}_procesado_nombres.csv"
df_nombres.to_csv(ruta_procesado_nombres, index=False)
print(f"El archivo CSV con nombres de dominio ha sido creado exitosamente en {ruta_procesado_nombres}.")

