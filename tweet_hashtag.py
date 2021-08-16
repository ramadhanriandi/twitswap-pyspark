import postgresql

# Get tweet hashtags
def get_tweet_hashtags(obj):
  hashtags = []

  if hasattr(obj, 'data') and hasattr(obj.data, 'entities') and len(obj.matching_rules) > 0:
    entities = obj.data.entities
    rule_id = str(obj.matching_rules[0].id)

    if hasattr(entities, 'hashtags'):
      hashtag_entities = entities.hashtags 

      for hashtag_entity in hashtag_entities:
        hashtag = hashtag_entity.tag
        new_hashtag = hashtag.lower() + '|' + rule_id
        
        if new_hashtag not in hashtags:
          hashtags.append(new_hashtag)

  return hashtags

# Insert tweet hashtags into a table
def insert_tweet_hashtags(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_hashtags = rdd.take(100)

  for data in tweet_hashtags:
    parsed_hashtag = data[0].split("|")

    name = parsed_hashtag[0]
    count = data[1]
    rule_id = parsed_hashtag[1]

    cursor.execute(
      """INSERT INTO tweet_hashtags(name, count, rule_id) VALUES (%s, %s, %s)""",
      (name, count, rule_id)
    )
  
  postgresql.close_connection_cursor(connection, cursor)