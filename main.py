import json
import psycopg2
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from types import SimpleNamespace

import config
import postgresql
import tweet_annotation
import tweet_coordinate
import tweet_domain
import tweet_hashtag
import tweet_language
import tweet_metric
import tweet_popularity
import tweet_source
import tweet_type
import tweet_word

# Process each line of the stream
def process_lines(lines):
  # Preprocess incoming tweet stream
  tweets = lines.map(lambda obj: obj[1]).filter(lambda line: len(line) >= 11)
  objects = tweets.map(lambda tweet: json.loads(tweet, object_hook = lambda d: SimpleNamespace(**d)))

  # Count for each tweet types
  converted_types = objects.map(tweet_type.get_tweet_type)
  tweet_types = converted_types.reduceByKey(lambda a, b: a + b)
  tweet_types.foreachRDD(tweet_type.insert_tweet_types)

  # Count for every domains
  converted_domains = objects.flatMap(tweet_domain.get_tweet_domains).map(lambda domain: (domain, 1))
  reduced_domains = converted_domains.reduceByKey(lambda a, b: a + b)
  tweet_domains = reduced_domains.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
  tweet_domains.foreachRDD(tweet_domain.insert_tweet_domains)

  # Count for every annotations
  converted_annotations = objects.flatMap(tweet_annotation.get_tweet_annotations).map(lambda annotation: (annotation, 1))
  reduced_annotations = converted_annotations.reduceByKey(lambda a, b: a + b)
  tweet_annotations = reduced_annotations.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
  tweet_annotations.foreachRDD(tweet_annotation.insert_tweet_annotations)

  # Count for every langs
  converted_langs = objects.map(tweet_language.get_tweet_lang)
  tweet_langs = converted_langs.reduceByKey(lambda a, b: a + b)
  tweet_langs.foreachRDD(tweet_language.insert_tweet_languages)

  # Get every coordinates
  converted_coordinates = objects.map(tweet_coordinate.get_tweet_coordinates)
  tweet_coordinates = converted_coordinates.filter(lambda coordinates: len(coordinates) == 3)
  tweet_coordinates.foreachRDD(tweet_coordinate.insert_tweet_coordinates)

  # Count for every hashtags
  converted_hashtags = objects.flatMap(tweet_hashtag.get_tweet_hashtags).map(lambda hashtag: (hashtag, 1))
  reduced_hashtags = converted_hashtags.reduceByKey(lambda a, b: a + b)
  tweet_hashtags = reduced_hashtags.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
  tweet_hashtags.foreachRDD(tweet_hashtag.insert_tweet_hashtags)

  # Count for every public metrics
  converted_metrics = objects.flatMap(tweet_metric.get_tweet_metrics)
  tweet_metrics = converted_metrics.reduceByKey(lambda a, b: a + b)
  tweet_metrics.foreachRDD(tweet_metric.insert_tweet_metrics)

  # Count for every popularities
  converted_popularities = objects.map(tweet_popularity.get_tweet_popularity)
  tweet_popularities = converted_popularities.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
  tweet_popularities.foreachRDD(tweet_popularity.insert_tweet_popularities)

  # Count for every words
  converted_words = objects.flatMap(tweet_word.get_tweet_words).map(lambda word: (word, 1))
  reduced_words = converted_words.reduceByKey(lambda a, b: a + b)
  tweet_words = reduced_words.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))
  tweet_words.foreachRDD(tweet_word.insert_tweet_words)

  # Count for every sources
  converted_sources = objects.map(tweet_source.get_tweet_source)
  tweet_sources = converted_sources.reduceByKey(lambda a, b: a + b)
  tweet_sources.foreachRDD(tweet_source.insert_tweet_sources)

  return tweet_sources

# Environment variables
APP_NAME = config.spark_app_name
MASTER = config.spark_master

KAFKA_TOPIC = config.kafka_topic
BOOTSTRAP_SERVER = config.kafka_bootstrap_server

# Spark configurations
conf = SparkConf() \
  .setAppName(APP_NAME) \
  .setMaster(MASTER)
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 10) # stream each ten second
ssc.checkpoint(config.spark_checkpoint)

# Consume Kafka topic
lines = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER})

# Process lines retrieved from Kafka topic
result = process_lines(lines)

# Print the result
result.pprint()

ssc.start()
ssc.awaitTermination()
