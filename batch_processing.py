from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Crear sesión de Spark
spark = SparkSession.builder.appName("MovieLensBatchProcessing").getOrCreate()

# Ruta del dataset
dataset_path = "./"

# Definir esquemas de los archivos
movies_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])

users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("occupation", IntegerType(), True),
    StructField("zip_code", StringType(), True)
])

# Cargar los archivos con Spark
movies_df = spark.read.text(dataset_path + "movies.dat")
ratings_df = spark.read.text(dataset_path + "ratings.dat")
users_df = spark.read.text(dataset_path + "users.dat")

# Separar las columnas correctamente usando "::" como delimitador
movies_df = movies_df.withColumn("movie_id", split(movies_df["value"], "::").getItem(0).cast(IntegerType())) \
                     .withColumn("title", split(movies_df["value"], "::").getItem(1)) \
                     .withColumn("genres", split(movies_df["value"], "::").getItem(2)) \
                     .drop("value")

ratings_df = ratings_df.withColumn("user_id", split(ratings_df["value"], "::").getItem(0).cast(IntegerType())) \
                       .withColumn("movie_id", split(ratings_df["value"], "::").getItem(1).cast(IntegerType())) \
                       .withColumn("rating", split(ratings_df["value"], "::").getItem(2).cast(IntegerType())) \
                       .withColumn("timestamp", split(ratings_df["value"], "::").getItem(3).cast(IntegerType())) \
                       .drop("value")

users_df = users_df.withColumn("user_id", split(users_df["value"], "::").getItem(0).cast(IntegerType())) \
                   .withColumn("gender", split(users_df["value"], "::").getItem(1)) \
                   .withColumn("age", split(users_df["value"], "::").getItem(2).cast(IntegerType())) \
                   .withColumn("occupation", split(users_df["value"], "::").getItem(3).cast(IntegerType())) \
                   .withColumn("zip_code", split(users_df["value"], "::").getItem(4)) \
                   .drop("value")

# Mostrar los primeros registros
print("Movies DataFrame:")
movies_df.show(5)

print("Ratings DataFrame:")
ratings_df.show(5)

print("Users DataFrame:")
users_df.show(5)

# Guardar DataFrames en formato parquet
movies_df.write.mode("overwrite").parquet("output/movies.parquet")
ratings_df.write.mode("overwrite").parquet("output/ratings.parquet")
users_df.write.mode("overwrite").parquet("output/users.parquet")

print("✅ Archivos guardados en formato Parquet en la carpeta 'output'.")

# Finalizar SparkSession
spark.stop()
