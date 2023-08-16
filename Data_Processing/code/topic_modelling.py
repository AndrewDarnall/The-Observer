# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#           PySpark Topic Modelling Data Enrichment
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.conf import SparkConf

from elasticsearch import Elasticsearch

import os
import random

# Elasticsearch details
elastic_host = os.getenv("ELASTIC_HOST_URL")

# Parametrically obtain the Kafka Topic
kafka_topic = os.getenv("KAFKA_TOPIC")
kafka_servers = os.getenv("KAFKA_SERVER_URL")

# Set the ElasticSearch Index to the same name as the Kafka_Topic for ease of use
elastic_index = kafka_topic


# Define the ElasticSearch mappings for the specifyed index
es_mapping = {
    "mappings": {
        "properties": 
            {
                "timestamp": {"type": "date"},
                "id": {"type": "long"},
                "topic": {"type": "text"},
                "created_at": {"type": "date"}, #, "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ"},
                "content": {"type": "text", "fielddata": True},
                "enrichment": {"type": "text"}
            }
    }
}


# ElasticSearch settings
es_settings = {
    "es.nodes": "localhost",
    "es.port": "9200",
    "es.resource": elastic_index,
    "es.write.operation": "index"
}


# Configuring the SparkSession with ElasticSearch
sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

pyspark_app_name = "topic_" + kafka_topic

# Creates the spark session for the spark app
spark = SparkSession.builder \
    .appName(name=pyspark_app_name) \
    .config(conf=sparkConf) \
    .getOrCreate()

# Defne the Elasticsearch server and index details

# Define the schema for the incoming data
schema = StructType([
    StructField("id", StringType()),
    StructField("created_at", StringType()),
    StructField("content", StringType())
])

# Read data from Kafka topic as a structured stream, Spark defines the stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# This expression selects certain columns, applying a typecast to the value column, thus polishing the input datastream
df = df.selectExpr("timestamp", "topic", "CAST(value AS STRING)")

# Further process the incoming dataframe
df = df.select("timestamp", "topic", from_json("value", schema).alias("data"))

# Exploded df
df = df.select("timestamp", "topic","data.id","data.created_at","data.content")

# Data enrichment portion
df = df.withColumn("enrichment", (col('id') * random.random()) % 5)
# ^ This is a dummy example to run some processing on the data stream


# -------------------- Output stream phase ------------------------ #

# Configuring the ElasticSearch cli
es = Elasticsearch(hosts=elastic_host,request_timeout=120) 

# Configuring the elastic-indeces
try:
    response = es.indices.create(index=elastic_index, mappings=es_mapping)
except Exception as e:
    print("ElasticSearch EXCEPTION raised:\t{}".format(e))

# ************************* For Debugging ************************* #
if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
    else:
        for i in range(10000):
            print("Elasticsearch index not created!")
# ************************* For Debugging ************************* #


# Output the processed data to an output sink ~ ElasticSearch in the project's case
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

# Stop the Spark session
spark.stop()