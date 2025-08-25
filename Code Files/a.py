from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit
from pyspark.sql.types import StructType, StringType, StructField, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Structured_Redpanda_WordCount") \
    .config("spark.jars", "s3://requiredfiles2323/spark-sql-kafka-0-10_2.12-3.2.1.jar,s3://requiredfiles2323/kafka-clients-2.1.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


class DistanceCalculator:
    @staticmethod
    def calculate_distance(lat1, lon1, lat2, lon2):
        """
        Calculates the Haversine distance between two points on the Earth.
        """
        R = 6371.0  # Radius of Earth in kilometers
        lat1_rad, lon1_rad = F.radians(lat1), F.radians(lon1)
        lat2_rad, lon2_rad = F.radians(lat2), F.radians(lon2)

        d_lat, d_lon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
        a = F.sin(d_lat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(d_lon / 2) ** 2
        c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
        return R * c


# Load historical data
histdf = spark.read.csv("s3://credit-project-data-80-percent/part-00000-5c9dab29-f21f-4f32-821a-4c9d76f329e8-c000.csv", header=True).cache()

# Schema for Kafka streaming data
schema = StructType([
    StructField("_c0", StringType(), True),
    StructField("ssn", StringType(), True),
    StructField("cc_num", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", DecimalType(5, 0), True),
    StructField("lat", DecimalType(9, 6), True),
    StructField("long", DecimalType(9, 6), True),
    StructField("city_pop", StringType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("acct_num", StringType(), True),
    StructField("profile", StringType(), True),
    StructField("trans_num", StringType(), True),
    StructField("trans_date", StringType(), True),
    StructField("trans_time", StringType(), True),
    StructField("unix_time", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DecimalType(9, 2), True),
    StructField("is_fraud", DecimalType(9, 0), True),
    StructField("merchant", StringType(), True),
    StructField("merch_lat", DecimalType(9, 6), True),
    StructField("merch_long", DecimalType(9, 6), True),
])

# Read streaming data from Kafka topic
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-p11xm.us-east-1.aws.confluent.cloud:9092") \
    .option("subscribe", "new") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username='NQUVTTMSN2ZERMOC' " +
            "password='H5D3zLCvpITwlG+erBnSp1uVtQkjYeZpFqoQ38XdO0lHymht4ny68cCZ7KT++S36';") \
    .load()

# Parse Kafka messages
df = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Read data from RDS
jdbc_url = "jdbc:mysql://grouptwo.cq7tmru9zell.us-east-1.rds.amazonaws.com/customer"
connection_properties = {
    "user": "admin",
    "password": "",
    "driver": "com.mysql.jdbc.Driver"
}
rds_df = spark.read.jdbc(jdbc_url, table="customer_details", properties=connection_properties)

# Calculate last 10 transactions and average transaction amount
window_spec = Window.partitionBy("cc_num").orderBy(F.col("unix_time").desc())
last_10_transactions = histdf.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") <= 10)
average_transaction_amount = last_10_transactions.groupBy("cc_num").agg(F.avg("amt").alias("avg_transaction_amt"))

# Filter and process streaming data
filtered_df = df.join(average_transaction_amount, "cc_num", "inner")
result = filtered_df.withColumn(
    "distance", DistanceCalculator.calculate_distance(col("lat"), col("long"), col("merch_lat"), col("merch_long"))
).withColumn(
    "status", when((col("distance") > 1500) & (col("amt") > col("avg_transaction_amt") + 500), "fraud").otherwise("genuine")
)

def write_to_jdbc_micro_batch(df, epoch_id):
    """
    Writes micro-batch data to RDS.
    """
    if not df.isEmpty():
        df.select("ssn", "cc_num", "first", "last", "gender", "city", "state", "dob", "acct_num", "is_fraud") \
          .write.jdbc(jdbc_url, table="customer_details", mode="append", properties=connection_properties)

# Write streaming results to JDBC
query1 = df.select("ssn", "cc_num", "first", "last", "gender", "city", "state", "dob", "acct_num", "is_fraud") \
    .writeStream.foreachBatch(write_to_jdbc_micro_batch).start()

# Write results to S3 as Parquet
query2 = result.writeStream.outputMode("append") \
    .foreachBatch(lambda batch_df, epoch_id: batch_df.drop("first", "last", "job", "profile")
                  .write.mode("append").parquet("s3://athenabucket-231/final.parquet/")) \
    .start()

# Await termination
query1.awaitTermination()
query2.awaitTermination()
