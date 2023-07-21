from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

is_aws = os.environ.get("AWS_ACCESS_KEY_ID") is not None

if is_aws:
    URL = 's3a://tantusdata/master-apache-spark/500K-products.2.5M-orders'
else:
    URL = 's3://tantusdata/master-apache-spark/500K-products.2.5M-orders/'

productsDf = spark.read.parquet(os.path.join(URL, 'products.parquet'))
ordersDf = spark.read.parquet(os.path.join(URL, 'orders.parquet'))

def crossJoinDefault():
    ordersDf.createOrReplaceTempView('orders')

    return spark.sql("""
        SELECT
            o1.orderID AS orderID, o1.productID AS productID1, o2.productID AS productID2
        FROM
            orders o1
        JOIN
            orders o2
        ON
            o1.orderID = o2.orderID AND o1.productID < o2.productID
    """)

def crossJoinOptimized():
    salt_df = spark.range(0, 18).withColumnRenamed('id', 'salt')

    ordersDfSalt = ordersDf.withColumn('salt', F.floor(F.rand() * 18))
    ordersDfCrossSalt = ordersDf.crossJoin(salt_df)

    ordersDfSalt.createOrReplaceTempView('orders_salt')
    ordersDfCrossSalt.createOrReplaceTempView('orders_cross_salt')

    return  spark.sql("""
        SELECT
            o1.orderID AS orderID, o1.productID AS productID1, o2.productID AS productID2
        FROM
            orders_salt o1
        JOIN
            orders_cross_salt o2
        ON
            o1.orderID = o2.orderID AND o1.productID < o2.productID AND o1.salt = o2.salt
    """)

def checkCorrectness(resultNormalDf, resultOptimizedDf):
    
    resultNormalDf.createOrReplaceTempView('table_resultNormal')
    resultOptimizedDf.createOrReplaceTempView('table_resultOptimized')

    checkIfEqualDf = spark.sql("""
        select count(*) from 
        (
         (
          SELECT * FROM table_resultNormal
          MINUS
          SELECT * FROM table_resultOptimized
         )
         UNION ALL
         (
          SELECT * FROM table_resultOptimized
          MINUS
          SELECT * FROM table_resultNormal
         )
        )
    """)

    checkIfEqualDf.show()

# replace with your own s3 bucket url
productsPairsDfDefault = crossJoinDefault()
productsPairsDfDefault.write.parquet(os.path.join('s3a://tantusdata/master-apache-spark/500K-products.2.5M-orders', 'productsPairsDfDefault.parquet'))
productsPairsDfOptimized = crossJoinOptimized()
productsPairsDfOptimized.write.parquet(os.path.join('s3a://tantusdata/master-apache-spark/500K-products.2.5M-orders', 'productsPairsDfOptimized.parquet'))

checkCorrectness(productsPairsDfDefault, productsPairsDfOptimized)