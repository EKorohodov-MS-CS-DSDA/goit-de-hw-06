import argparse
from configs import kafka_config, ALERTS_CONDITIONS_FILE, INPUT_TOPIC_BASE, OUTPUT_TOPIC_BASE
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql import SparkSession
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Константи
INVALID_DATA_VALUE = -999

# Функція для запису даних у Kafka
def write_to_kafka(df, epoch_id):
    df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("kafka.security.protocol",kafka_config['security_protocol']) \
        .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
        .option("kafka.sasl.jaas.config", kafka_config['kafka.sasl.jaas.config']) \
        .option("topic", f"{OUTPUT_TOPIC_BASE}_{args.topic_tag}") \
        .save()

# Функція для виведення даних у консоль (для налагодження)
def write_stream_to_console(df):
    df.writeStream \
        .trigger(availableNow=True) \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", "./chk/alerts_console") \
        .start() \
        .awaitTermination()


def main(args=None):
    
    # Створення SparkSession
    spark = (SparkSession.builder
            .appName("KafkaStreaming")
            .master("local[*]")
            .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # Читання потоку даних із Kafka
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("kafka.security.protocol", kafka_config['security_protocol']) \
        .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
        .option("kafka.sasl.jaas.config", kafka_config['kafka.sasl.jaas.config']) \
        .option("subscribe", f"{INPUT_TOPIC_BASE}_{args.topic_tag}") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "5") \
        .load()

    json_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    # Читання таблиці умов з CSV файлу
    conditions_pd = pd.read_csv(ALERTS_CONDITIONS_FILE)
    conditions_sdf = spark.createDataFrame(conditions_pd)

    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

    parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Агрегація даних за вікнами часу
    agg_df = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window("timestamp", "1 minute", "30 seconds")
        ).agg(
            round(avg("temperature"), 2).alias("t_avg"),
            round(avg("humidity"), 2).alias("h_avg")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("t_avg"),
            col("h_avg")
        )

    # Перетин з умовами для генерації алертів
    joined = agg_df.crossJoin(conditions_sdf)

    alerts_df = joined.filter(
        ((col("temperature_min") == INVALID_DATA_VALUE) | (col("t_avg") >= col("temperature_min"))) &
        ((col("temperature_max") == INVALID_DATA_VALUE) | (col("t_avg") <= col("temperature_max"))) &
        ((col("humidity_min") == INVALID_DATA_VALUE) | (col("h_avg") >= col("humidity_min"))) &
        ((col("humidity_max") == INVALID_DATA_VALUE) | (col("h_avg") <= col("humidity_max")))
    )

    # Формування вмісту алертів
    alerts_df = alerts_df.withColumn(
            "window", 
            struct(col("window_start").alias("start"), col("window_end").alias("end"))
        ).select(
        col("window"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message")
    )

    alerts_json = alerts_df.selectExpr(
        "CAST(code AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # Запис алертів у Kafka
    alerts_json.writeStream \
        .foreachBatch(write_to_kafka) \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic-tag", type=str, 
        default="ek_1", help="topic suffix. Default: 'ek_1'")

    args = parser.parse_args()
    main(args)