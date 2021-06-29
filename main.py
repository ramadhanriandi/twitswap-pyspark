import json
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from types import SimpleNamespace

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

def get_tweet_domains(data):
  domains = []

  if hasattr(data, 'context_annotations'):
    context_annotations = data.context_annotations

    for context_annotation in context_annotations:
      domain = context_annotation.domain.name

      if domain not in domains:
        domains.append(domain)

  return domains

def get_tweet_annotations(data):
  annotations = []

  if hasattr(data, 'entities'):
    entities = data.entities

    if hasattr(entities, 'annotations'):
      annotation_entities = entities.annotations 

      for annotation_entity in annotation_entities:
        annotation = annotation_entity.normalized_text

        if annotation not in annotations:
          annotations.append(annotation)

  return annotations

def get_tweet_lang(data):
  if hasattr(data, 'lang'):
    tweet_lang = data.lang

    if tweet_lang == "en" or tweet_lang == "in":
      return (tweet_lang, 1)

  return ("other", 1)
  
def process_lines(lines):
  tweets = lines.map(lambda obj: obj[1]).filter(lambda line: len(line) >= 11)
  objects = tweets.map(lambda tweet: json.loads(tweet, object_hook = lambda d: SimpleNamespace(**d)))
  datas = objects.map(lambda obj: obj.data) 

  # Count for each tweet types (tweet, retweet, quote, reply)
  converted_tweets = datas.map(get_tweet_type)
  tweet_types = converted_tweets.reduceByKey(lambda a, b: a + b)

  # Count for every domains
  converted_domains = datas.flatMap(get_tweet_domains).map(lambda domain: (domain, 1))
  tweet_domains = converted_domains.reduceByKey(lambda a, b: a + b)

  # Count for every annotations
  converted_annotations = datas.flatMap(get_tweet_annotations).map(lambda annotation: (annotation, 1))
  tweet_annotations = converted_annotations.reduceByKey(lambda a, b: a + b)

  # Count for every langs
  converted_langs = datas.map(get_tweet_lang)
  tweet_langs = converted_langs.reduceByKey(lambda a, b: a + b)

  return tweet_langs

# Environment variables
APP_NAME = "TwitSwap - PySpark"
MASTER = "local"

KAFKA_TOPIC = "raw-tweet-topic"
BOOTSTRAP_SERVER = "localhost:9092"

# Spark configurations
conf = SparkConf() \
  .setAppName(APP_NAME) \
  .setMaster(MASTER)
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 10) # stream each ten second
ssc.checkpoint("./checkpoint")

# Consume Kafka topic
lines = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER})

# Process lines retrieved from Kafka topic
result = process_lines(lines)

# Print the result
result.pprint()

ssc.start()
ssc.awaitTermination()
