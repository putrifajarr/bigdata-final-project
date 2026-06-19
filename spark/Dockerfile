FROM bitnamilegacy/spark:3.5

USER root

# Set HOME ke /tmp agar Java tidak kebingungan saat mencari home directory (menghindari error ?/.ivy2)
ENV HOME=/tmp

# Tambahkan konfigurasi default Spark agar Ivy cache selalu mengarah ke /tmp/.ivy2 yang absolut
RUN echo "spark.jars.ivy /tmp/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf

# Buat dummy script untuk trigger download via spark-submit
RUN echo "print('Downloading dependencies...')" > /tmp/dummy.py

# Gunakan fitur --packages bawaan Spark untuk resolve dan download semua JAR beserta transitive dependencies-nya
RUN spark-submit \
    --packages "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0,com.clickhouse:clickhouse-http-client:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.3,com.clickhouse:clickhouse-jdbc:0.6.3" \
    /tmp/dummy.py

# Pindahkan semua JAR yang berhasil di-download oleh Ivy ke direktori standar Spark jars
RUN cp /tmp/.ivy2/jars/*.jar /opt/bitnami/spark/jars/ && \
    rm -rf /tmp/.ivy2 && \
    chown -R 1001:1001 /opt/bitnami/spark/jars/

# Kembali ke user non-root standar Bitnami demi keamanan
USER 1001
