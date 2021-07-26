import postgresql

# Get tweet metrics
def get_tweet_metrics(obj):
  metrics = []

  if hasattr(obj.data, 'public_metrics') and len(obj.matching_rules) > 0:
    rule_id = str(obj.matching_rules[0].id)

    metrics.append(('retweet_count|' + rule_id, obj.data.public_metrics.retweet_count))
    metrics.append(('reply_count|' + rule_id, obj.data.public_metrics.reply_count))
    metrics.append(('like_count|' + rule_id, obj.data.public_metrics.like_count))
    metrics.append(('quote_count|' + rule_id, obj.data.public_metrics.quote_count))

  return metrics

# Insert tweet metrics into a table
def insert_tweet_metrics(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_metrics = rdd.take(4)

  retweet_count = tweet_metrics[0][1]
  reply_count = tweet_metrics[1][1]
  like_count = tweet_metrics[2][1]
  quote_count = tweet_metrics[3][1]

  rule_id = tweet_metrics[0][0].split("|")[1]

  cursor.execute(
    """INSERT INTO tweet_metrics(retweet_count, reply_count, like_count, quote_count, rule_id) VALUES (%s, %s, %s, %s, %s)""",
    (retweet_count, reply_count, like_count, quote_count, "rule_id_dummy")
    # (retweet_count, reply_count, like_count, quote_count, rule_id)
  )
  
  postgresql.close_connection_cursor(connection, cursor)