import findspark 
findspark.init()
import zipfile
from pyspark.sql.functions import col, lit, explode
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count
spark=SparkSession.builder.appName('Basics').getOrCreate()
import os
path = "/bg/tarea2/crossref.zip"
if not os.path.exists("/bg/tarea2/crossref"):
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall("/bg/tarea2/crossref")
files="/bg/tarea2/crossref/crossref"
if not os.path.exists("/bg/tarea2/output/crossref-parquet"):
    df=spark.read.option("multiline", "true").json(files)
    df_transformacion=df.select(
        col("message.DOI").alias("doi"),
        col("message.author").alias("author"),
        col("message.institution").alias("inst"),
        col("message.group-title").alias("groupTitle"),
        col("message.created.date-time").alias("createdDate"),
        col("message.prefix").alias("prefix"),
        col("message.reference").alias("reference"),
        col("message.link").alias("link"),
        col("message.subtype").alias("subtype")
    )
    df_transformacion.printSchema()
    df_transformacion.write.mode("overwrite").parquet("/bg/tarea2/output/crossref-parquet")

df_parquet=spark.read.parquet("/bg/tarea2/output/crossref-parquet")
df_parquet.printSchema()
df_year_month=df_parquet.withColumn("created_year",year(col("createdDate"))).withColumn("created_month",month(col("createdDate")))
df_year_month_grouped=df_year_month.groupBy("created_year","created_month")
df_dates=df_year_month_grouped.agg(count("created_year").alias("total_articles"))
df_dates.show()
df_dates.write.mode("overwrite").csv("/bg/tarea2/output/articles_month_year")
df_dates.printSchema()