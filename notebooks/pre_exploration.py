import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(
    host="13.215.79.3", 
    port=8123,
    username="default", 
    password="rahasia123",
    database="flight_delay"
)

# Lihat struktur kolom ontime_features
df_sample = client.query_df("""
    SELECT *
    FROM ontime_features
    LIMIT 5
""")

print(df_sample.to_string())
print("\nKolom yang tersedia:")
print(df_sample.dtypes)