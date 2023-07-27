from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
# from pyspark.sql.functions import from_json
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark import SparkContext
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import *

elastic_host="http://elasticsearch:9200"
elastic_index="mastodonuno"
kafkaServer="kafkaServer:9092"
topic = "dataflow"

# Fix the mappings
es_mapping = {
    "mappings": {
        "properties": 
            {
                "id": {"type": "long"},
                # "created_at": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ"},
                "content": {"type": "text", "fielddata": True}
            }
    }
}

print("----------------- UPDATE: 10 -------------------")

# Tweak the timeout value for connectivity errors
es = Elasticsearch(hosts=elastic_host,request_timeout=60) 
# make an API call to the Elasticsearch cluster
# and have it return a response:

response = es.indices.create(index=elastic_index,body=es_mapping)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])
    else:
        for i in range(10000):
            print("Elasticsearch index not created!")

# Define Training Set Structure
tootKafka = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    # tp.StructField(name= 'created_at', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'content',       dataType= tp.StringType(),  nullable= True)
])

# Training Set Schema
schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subjective',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'positive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'negative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'ironic',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lpositive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lnegative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'top',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'content',       dataType= tp.StringType(),   nullable= True)
])

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

sc = SparkContext(appName="TheObserver", conf=sparkConf)
spark = SparkSession(sc)

# Reduce the verbosity of logging messages
sc.setLogLevel("ERROR")

# Kafka topic subscription
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaServer).option("subscribe", topic).load()


# This is where the actual pre-processing happens
df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", tootKafka).alias("data")).select("data.*")

# This is the stream query to apply
query = df.writeStream.format("console").start()

# Write the stream to elasticsearch
df.writeStream \
    .option("checkpointLocation", "/save/location") \
    .format("es") \
    .start(elastic_index) \
    .awaitTermination()