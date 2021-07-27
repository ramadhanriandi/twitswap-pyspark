import postgresql

# Get tweet popularity
def get_tweet_popularity(obj):
  if hasattr(obj, 'data') and hasattr(obj.data, 'id') and len(obj.matching_rules) > 0:
    popularity = 0

    if hasattr(obj.data, 'public_metrics'):
      popularity += obj.data.public_metrics.retweet_count
      popularity += obj.data.public_metrics.reply_count
      popularity += obj.data.public_metrics.like_count
      popularity += obj.data.public_metrics.quote_count

    return (obj.data.id, popularity, obj.matching_rules[0].id)

  return ('', 0, '')

# Insert tweet popularity into a table
def insert_tweet_popularities(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_popularities = rdd.take(3)

  for data in tweet_popularities:
    tweet_id = data[0]
    popularity = data[1]
    rule_id = data[2]

    if len(tweet_id) > 0 and popularity > 0:
      cursor.execute(
        """INSERT INTO tweet_popularities(tweet_id, popularity, rule_id) VALUES (%s, %s, %s)""",
        (tweet_id, popularity, "rule_id_dummy")
        # (tweet_id, popularity, rule_id)
      )
  
  postgresql.close_connection_cursor(connection, cursor)