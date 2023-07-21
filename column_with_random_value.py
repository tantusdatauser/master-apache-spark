from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# replace with your own s3 bucket url
PATH = 's3a://tantusdata/master-apache-spark/users-df.parquet'

df = sc.parallelize(range(10_000_000), 1) \
	.map(lambda i: (f"User_{i}",)) \
	.toDF(['userID'])

df.write.parquet(PATH, mode='overwrite')

df = spark.read.parquet(PATH)

dfWithRandom = df \
	.repartition(10) \
	.withColumn('random', F.rand(seed=42))


df1 = dfWithRandom.filter(F.col('random') < 0.5)
df2 = dfWithRandom.filter(F.col('random') >= 0.5)
		
df1.write.parquet(PATH + '.1', mode='overwrite')
df2.write.parquet(PATH + '.2', mode='overwrite')

df1 = spark.read.parquet(PATH + '.1')
df2 = spark.read.parquet(PATH + '.2')

df3 = df1.select('userID').intersect(df2.select('userID'))
df3.head(10)