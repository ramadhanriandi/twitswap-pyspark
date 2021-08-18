import postgresql

# Get tweet source
def get_tweet_source(obj):
  if hasattr(obj, 'data') and hasattr(obj.data, 'source') and len(obj.matching_rules) > 0:
    tweet_source = obj.data.source
    rule_id = str(obj.matching_rules[0].id)

    source = "other"
    
    if tweet_source == "Twitter Web App":
      source = "web"
    elif tweet_source == "Twitter for iPhone":
      source = "iphone"
    elif tweet_source == "Twitter for Android":
      source = "android"

    return (source + "|" + rule_id, 1)
  
  return ("", 0)

# Insert tweet sources into a table
def insert_tweet_sources(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_sources = rdd.take(4)

  if len(tweet_sources) > 0:
    web_count = 0
    iphone_count = 0
    android_count = 0
    other_count = 0
    rule_id = ""

    for data in tweet_sources:
      if len(data[0]) > 0:
        parsed_source = data[0].split("|")

        if len(parsed_source) == 2:
          source = parsed_source[0]
          count = data[1]
          rule_id = parsed_source[1]

          if source == "web":
            web_count = count
          elif source == "iphone":
            iphone_count = count
          elif source == "android":
            android_count = count
          elif source == "other":
            other_count = count

    if len(rule_id) > 0:
      cursor.execute(
        """INSERT INTO tweet_sources(web_count, iphone_count, android_count, other_count, rule_id) VALUES (%s, %s, %s, %s, %s)""",
        (web_count, iphone_count, android_count, other_count, rule_id)
      )
  
  postgresql.close_connection_cursor(connection, cursor)