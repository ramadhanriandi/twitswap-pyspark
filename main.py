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
import tweet_domain
import tweet_hashtag
import tweet_metric
import tweet_popularity

# Get tweet type
def get_tweet_type(data):
  if hasattr(data, 'referenced_tweets'):
    tweet_type = data.referenced_tweets[0].type

    if tweet_type == "retweeted":
      return ("retweet", 1)
    elif tweet_type == "quoted":
      return ("quote", 1)
    elif tweet_type == "replied_to":
      return ("replied_to", 1)

  return ("tweet", 1)

# Get tweet language
def get_tweet_lang(data):
  if hasattr(data, 'lang'):
    tweet_lang = data.lang

    if tweet_lang == "en" or tweet_lang == "in":
      return (tweet_lang, 1)

  return ("other", 1)

# Get tweet coordinates
def get_tweet_coordinates(data):
  if (hasattr(data, 'geo')):
    if (hasattr(data.geo, 'coordinates')):
      return data.geo.coordinates.coordinates

  return []

# Process each line of the stream
def process_lines(lines):
  # Preprocess incoming tweet stream
  tweets = lines.map(lambda obj: obj[1]).filter(lambda line: len(line) >= 11)
  objects = tweets.map(lambda tweet: json.loads(tweet, object_hook = lambda d: SimpleNamespace(**d)))
  datas = objects.map(lambda obj: obj.data)

  # Count for each tweet types (tweet, retweet, quote, reply)
  converted_types = datas.map(get_tweet_type)
  tweet_types = converted_types.reduceByKey(lambda a, b: a + b)

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
  converted_langs = datas.map(get_tweet_lang)
  tweet_langs = converted_langs.reduceByKey(lambda a, b: a + b)

  # Get every coordinates
  converted_coordinates = datas.map(get_tweet_coordinates)
  tweet_coordinates = converted_coordinates.filter(lambda coordinates: len(coordinates) == 2)

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
  tweet_popularities.foreachRDD(tweet_popularity.insert_tweet_popularity)

  return tweet_hashtags

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
