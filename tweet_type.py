import postgresql

# Get tweet type
def get_tweet_type(obj):
  if hasattr(obj, 'data') and len(obj.matching_rules) > 0:
    rule_id = str(obj.matching_rules[0].id)
    
    if hasattr(obj.data, 'referenced_tweets'):
      tweet_type = obj.data.referenced_tweets[0].type

      if tweet_type == "retweeted":
        return ("retweet" + "|" + rule_id, 1)
      elif tweet_type == "quoted":
        return ("quote" + "|" + rule_id, 1)
      elif tweet_type == "replied_to":
        return ("replied_to" + "|" + rule_id, 1)

    return ("tweet" + "|" + rule_id, 1)
  
  return ("", 0)

# Insert tweet types into a table
def insert_tweet_types(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_types = rdd.take(5)

  if len(tweet_types) > 0:
    tweet_count = 0
    retweet_count = 0
    quote_count = 0
    reply_count = 0
    rule_id = ""

    for data in tweet_types:
      if len(data[0]) > 0:
        parsed_type = data[0].split("|")

        tweet_type = parsed_type[0]
        count = data[1]
        rule_id = parsed_type[1]

        if tweet_type == "tweet":
          tweet_count = count
        elif tweet_type == "retweet":
          retweet_count = count
        elif tweet_type == "quote":
          quote_count = count
        elif tweet_type == "replied_to":
          reply_count = count

    if len(rule_id) > 0:
      cursor.execute(
        """INSERT INTO tweet_types(tweet_count, retweet_count, quote_count, reply_count, rule_id) VALUES (%s, %s, %s, %s, %s)""",
        (tweet_count, retweet_count, quote_count, reply_count, rule_id)
      )
  
  postgresql.close_connection_cursor(connection, cursor)