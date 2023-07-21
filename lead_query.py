import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

import os

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

is_aws = os.environ.get("AWS_ACCESS_KEY_ID") is not None

if is_aws:
    URL = 's3://tantusdata/master-apache-spark/100K-users.1B-events-with-timestamp.skew9-1'
else:
    URL = 's3a://tantusdata/master-apache-spark/100K-users.1B-events-with-timestamp.skew9-1'

eventsDf = spark.read.parquet(os.path.join(URL, 'events.parquet'))

def leadQueryDefault():

    eventsDf.createOrReplaceTempView('table_events')

    return spark.sql("""
		SELECT 
			eventID, userID, eventTimestamp, LEAD(eventTimestamp, 1) OVER (
				PARTITION BY 
					userID 
				ORDER BY 
					eventTimestamp
			) AS nextEvent 
		FROM 
			table_events 
	""")

def leadQueryOptimized():

    N = 24*3600*35 # 35 days in seconds
    eventsDfWithPid = eventsDf \
        .withColumn('pid', F.ceil(F.unix_timestamp('eventTimestamp')/N))
    
    w = Window.partitionBy('userID').orderBy('eventTimestamp')
    w1 = Window.partitionBy('userID', 'pid').orderBy('eventTimestamp')
    w2 = Window.partitionBy('userID', 'pid')

    eventsLeadDfWithNulls = eventsDfWithPid.select(
        '*',
        F.count('*').over(w2).alias('count'),
        F.row_number().over(w1).alias('row_number'),
        F.lead('eventTimestamp', 1).over(w1).alias('nextEvent')
    )

    eventsNullDfFixed = eventsLeadDfWithNulls \
        .filter('row_number in (1, count)') \
        .withColumn('nextEvent', F.lead('eventTimestamp', 1).over(w)) \
        .filter('row_number = count') \
        .drop('count', 'row_number', 'pid', 'eventData')

    eventsLeadDf = eventsLeadDfWithNulls \
        .filter('row_number <> count') \
        .drop('count', 'row_number', 'pid', 'eventData') \
        .unionAll(eventsNullDfFixed)
    
    return eventsLeadDf

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
resultNormalDf = leadQueryDefault()
resultNormalDf.write.parquet(os.path.join('s3a://tantusdata/master-apache-spark/100K-users.1B-events-with-timestamp.skew9-1', 'leadQueryDefault.parquet'))
resultOptimizedDf = leadQueryOptimized()
resultOptimizedDf.write.parquet(os.path.join('s3a://tantusdata/master-apache-spark/100K-users.1B-events-with-timestamp.skew9-1', 'leadQueryOptimized.parquet'))

checkCorrectness(resultNormalDf, resultOptimizedDf)