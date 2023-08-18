# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#           PySpark Topic Modelling Data Enrichment
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.conf import SparkConf
from pyspark.sql.functions import udf

from elasticsearch import Elasticsearch

import os
import random

import re
from html import unescape

from bertopic import BERTopic
import joblib
import torch


import scipy
import scipy.sparse


# Machine Learning model setup
# model_path = os.getcwd() + '/CPU_job_trained_bert_model.pkl'
model_path = os.getcwd() + '/CPU_job_non_condensed_trained_bert_model.pkl'

# Load the model (beware, the deserialization might be troublesome if trained on a CUDA device i.e. google colab T4 GPU)
# topic_model = BERTopic.load(model_path) 

# Alternatively
topic_model = joblib.load(model_path)


# HTML Text parser function
def remove_html_tags(text):

    # Use regular expression to remove HTML tags and related content
    try:
        text = re.sub(r'<[^>]+>', '', text)
    except Exception as e:
        print("Parser exception caught:\t{}".format(e))
        return text
    try:
        text = unescape(text)
    except Exception as e:
        print("HTML escaping exception caught:\t{}".format(e))
        return text
    
    return text


###########################################################################################
#                                           SETUP                                         #
###########################################################################################

# Elasticsearch details
elastic_host = os.getenv("ELASTIC_HOST_URL")
elastic_server = os.getenv("ELASTIC_SERVER_HOSTNAME")

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
                # "cleaned_content": {"type": "text", "fielddata": True},
                "content": {"type": "text", "fielddata": True},
                # Tweak the enrichment at will
                "enrichment": {"type": "long"}
            }
    }
}


# ElasticSearch settings
es_settings = {
    "es.nodes": elastic_server,
    "es.port": "9200",
    "es.nodes.wan.only": "true",
    "es.resource": elastic_index,
    "es.mapping.id": "id",
    "es.write.operation": "index"
}


###########################################################################################
#                                   Spark App Config                                      #
###########################################################################################

# Configuring the SparkSession with ElasticSearch
sparkConf = SparkConf() \
        .set('spark.streaming.stopGracefullyOnShutdown', 'true') \
        .set('spark.streaming.kafka.consumer.cache.enabled', 'false') \
        .set('spark.streaming.backpressure.enabled', 'true') \
        .set('spark.streaming.kafka.maxRatePerPartition', '100') \
        .set('spark.streaming.kafka.consumer.poll.ms', '512') \
        .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1') \
        .set('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint')

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


# <====================== UDF ==========================> #
remove_html_udf = udf(remove_html_tags, StringType())

# Processing the noisy text data
df = df.withColumn("content", remove_html_udf(col('content')))


# -------------------- Output stream phase ------------------------ #

# Configuring the ElasticSearch cli
es = Elasticsearch(hosts=elastic_host,request_timeout=120) 

# Configuring the elastic-indeces
try:
    response = es.indices.create(index=elastic_index, body=es_mapping, ignore=400)
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
    .format("es") \
    .options(**es_settings) \
    .start()




# For debugging & visualization
query_2 = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# For debugging & visualization




# Wait for the query to terminate
query.awaitTermination()



# For debugging only
query_2.awaitTermination()
# For debugging only




# Stop the Spark session
spark.stop()