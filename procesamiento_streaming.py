from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaStreamingProcessing") \
    .getOrCreate()

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "realtime_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir el valor de los mensajes a String
df_parsed = df.selectExpr("CAST(value AS STRING) as message", "timestamp")

# Contar mensajes en ventanas de 10 segundos
event_count = df_parsed.groupBy(window(col("timestamp"), "10 seconds")) \
    .agg(count("*").alias("event_count"))

# Mostrar los resultados en consola en tiempo real
query = event_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Esperar que la consulta termine
query.awaitTermination()
