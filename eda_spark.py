from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count

# Crear sesión de Spark
spark = SparkSession.builder.appName("MovieLensEDA").getOrCreate()

# Cargar los datos desde Parquet
movies_df = spark.read.parquet("output/movies.parquet")
ratings_df = spark.read.parquet("output/ratings.parquet")
users_df = spark.read.parquet("output/users.parquet")

# 1️⃣ LIMPIEZA DE DATOS
movies_df = movies_df.dropna().dropDuplicates()
ratings_df = ratings_df.dropna().dropDuplicates()
users_df = users_df.dropna().dropDuplicates()

# 2️⃣ TRANSFORMACIÓN: Crear una nueva columna de rango de edad
users_df = users_df.withColumn(
    "age_group",
    when(col("age") < 18, "Menor de edad")
    .when((col("age") >= 18) & (col("age") <= 35), "Joven")
    .when((col("age") > 35) & (col("age") <= 60), "Adulto")
    .otherwise("Mayor")
)

# 3️⃣ ANÁLISIS EXPLORATORIO (EDA)

# a) Número total de películas, usuarios y ratings
num_movies = movies_df.count()
num_users = users_df.count()
num_ratings = ratings_df.count()
print(f" Total de películas: {num_movies}")
print(f" Total de usuarios: {num_users}")
print(f" Total de ratings: {num_ratings}")

# b) Rating promedio por película
ratings_avg_df = ratings_df.groupBy("movie_id").agg(avg("rating").alias("avg_rating"))
print(" Películas con mejor rating:")
ratings_avg_df.orderBy(col("avg_rating").desc()).show(10)

# c) Distribución de usuarios por grupo de edad
print(" Distribución de usuarios por grupo de edad:")
users_df.groupBy("age_group").count().show()

# 4️⃣ ALMACENAR LOS RESULTADOS PROCESADOS
users_df.write.mode("overwrite").parquet("output/clean_users.parquet")
ratings_avg_df.write.mode("overwrite").parquet("output/ratings_avg.parquet")

print(" Proceso de limpieza, transformación y análisis exploratorio completado.")
