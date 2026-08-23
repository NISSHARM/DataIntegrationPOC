# Databricks notebook source
kafkaBootstrapServers = dbutils.secrets.get(scope = "kyvazew_anl009aliancabi", key = "kafka-retina-bootstrap-servers")
truststorePassword = dbutils.secrets.get(scope = "kyvazew_anl009aliancabi", key = "TruststorePasswordEcargoP")
topicName="anl.document.move.topic.internal.any.v2"
kafkaUser = dbutils.secrets.get(scope = "kyvazew_anl009aliancabi", key = "kafka-retina-user-EcargoP")
kafkaSecret = dbutils.secrets.get(scope = "kyvazew_anl009aliancabi", key = "kafka-retina-secret-EcargoP")

# COMMAND ----------

# DBTITLE 1,Connecting to topic
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",kafkaBootstrapServers)
    .option("subscribe",topicName)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafkaUser}" password="{kafkaSecret}";')
    .option("kafka.ssl.truststore.location", "/dbfs/tmp/kafka/ca.p12")
    .option("kafka.ssl.truststore.password", truststorePassword)
    .option("kafka.ssl.truststore.type", "PKCS12")
    .load()
)

# COMMAND ----------

# DBTITLE 1,Deserailize
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F
avro_schema = """
{
  "type": "record",
  "name": "MyData",
  "fields": [
    {"name": "ShipmentNumber", "type": "string"},
	{"name": "ContainerNumber", "type": "string"},
	{"name": "MoveType", "type": "string"},
	{"name": "LegNo", "type": "string"},
	{"name": "LfLegNo", "type": "string"},
	{"name": "LocationMrc", "type": "string"},
	{"name": "LocationGeo", "type": "string"},
	{"name": "ActivityDate", "type": "string"},
	{"name": "Vessel", "type": "string"},
	{"name": "EquipGroup", "type": "string"},
	{"name": "EquipmentSize", "type": "string"},
	{"name": "EquipmentType", "type": "string"},
	{"name": "Status", "type": "string"},
	{"name": "timestamp", "type": "string"}
  ]
}
"""
df_decoded = df_raw.select(
    from_avro(F.col("value"), avro_schema).alias("data")
).select("data.*")


# COMMAND ----------

display(df_decoded)

# COMMAND ----------

import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
try:
    s.connect(("emp-kafka-shared-prodwe-retina.maersk-digital.net", 443))  # use your actual broker + port
    print("✅ Can reach broker")
except Exception as e:
    print("❌ Cannot reach broker:", e)

# COMMAND ----------

df_test = (
    spark.read              # <-- read, not readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",kafkaBootstrapServers)
    .option("subscribe", topicName)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

df_decoded_test = df_test.select(
    from_avro(F.col("value"), avro_schema).alias("data")
).select("data.*")



# COMMAND ----------



# COMMAND ----------

display(df_decoded_test.limit(1))

# COMMAND ----------

df_raw_test = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("subscribe", topicName)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1")   # <-- only pulls 1 message
    .load()
)

df_decoded_test = df_raw_test.select(
    from_avro(F.col("value"), avro_schema).alias("data")
).select("data.*")

query = (
    df_decoded_test.writeStream
    .format("memory")            # temp in-memory table, just for viewing
    .queryName("test_view")
    .trigger(availableNow=True)  # run once, then stop
    .start()
)

query.awaitTermination()



# COMMAND ----------

display(spark.sql("SELECT * FROM test_view"))

# COMMAND ----------

import socket
host = "emp-kafka-shared-ppwe-retina.maersk-digital.net"
port = 443

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
try:
    s.connect((host, port))
    print("✅ Can reach broker at network level")
except Exception as e:
    print("❌ Cannot reach broker:", e)

# COMMAND ----------

from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F


# ---- Step 1: Connect to Kafka (SASL_SSL, since port is 443) ----
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("subscribe", topicName)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1")   # only pull 1 message, for quick testing
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafkaUser}" password="{kafkaSecret}";')
    .load()
)

# ---- Step 2: Avro schema ----
avro_schema = """
{
  "type": "record",
  "name": "MyData",
  "fields": [
    {"name": "ShipmentNumber", "type": "string"},
    {"name": "ContainerNumber", "type": "string"},
    {"name": "MoveType", "type": "string"},
    {"name": "LegNo", "type": "string"},
    {"name": "LfLegNo", "type": "string"},
    {"name": "LocationMrc", "type": "string"},
    {"name": "LocationGeo", "type": "string"},
    {"name": "ActivityDate", "type": "string"},
    {"name": "Vessel", "type": "string"},
    {"name": "EquipGroup", "type": "string"},
    {"name": "EquipmentSize", "type": "string"},
    {"name": "EquipmentType", "type": "string"},
    {"name": "Status", "type": "string"},
    {"name": "timestamp", "type": "string"}
  ]
}
"""

# ---- Step 3: Decode Avro ----
df_decoded = df_raw.select(
    from_avro(F.col("value"), avro_schema).alias("data")
).select("data.*")

# ---- Step 4: Run once, dump to a temp in-memory table, view it ----
query = (
    df_decoded.writeStream
    .format("memory")
    .queryName("test_view")
    .trigger(availableNow=True)   # runs once and stops — no infinite streaming
    .start()
)

query.awaitTermination()

display(spark.sql("SELECT * FROM test_view"))