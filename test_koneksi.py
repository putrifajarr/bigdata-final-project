import clickhouse_connect

client = clickhouse_connect.get_client(
    host="13.215.79.3",
    port=8123,
    username="default",
    password="rahasia123",
    database="flight_delay"
)

# Tes 1: Cek koneksi
result = client.query("SELECT count() FROM ontime_features")
print("✅ Koneksi berhasil!")
print(f"   Jumlah data di ontime_features: {result.first_row[0]:,} baris")

# Tes 2: Cek tabel tersedia
result2 = client.query("SHOW TABLES FROM flight_delay")
tables = [row[0] for row in result2.result_rows]
print(f"\n📦 Tabel yang tersedia ({len(tables)} tabel):")
for t in sorted(tables):
    print(f"   - {t}")