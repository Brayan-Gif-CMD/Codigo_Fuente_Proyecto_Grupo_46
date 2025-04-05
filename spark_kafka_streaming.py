from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

# Leer datos de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "realtime_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir los valores de Kafka a String
messages_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Dividir los mensajes por "::" para obtener los campos
movies_df = messages_df.withColumn("movie_id", split(col("value"), "::")[0]) \
                       .withColumn("title", split(col("value"), "::")[1]) \
                       .withColumn("genres", split(col("value"), "::")[2])

# Mostrar los datos en la consola
query = movies_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
