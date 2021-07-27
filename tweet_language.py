import postgresql

# Get tweet language
def get_tweet_lang(obj):
  if hasattr(obj, 'data') and hasattr(obj.data, 'lang') and len(obj.matching_rules) > 0:
    tweet_lang = obj.data.lang
    rule_id = str(obj.matching_rules[0].id)

    if tweet_lang == "en" or tweet_lang == "in":
      return (tweet_lang + "|" + rule_id, 1)

    return ("other" + "|" + rule_id, 1)
  
  return ("", 0)

# Insert tweet language into a table
def insert_tweet_language(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_languages = rdd.take(3)

  if len(tweet_languages) > 0:
    en_count = 0
    in_count = 0
    other_count = 0
    rule_id = ""

    for data in tweet_languages:
      if len(data) > 0:
        parsed_language = data[0].split("|")

        language = parsed_language[0]
        count = data[1]
        rule_id = parsed_language[1]

        if language == "en":
          en_count = count
        elif language == "in":
          in_count = count
        elif language == "other":
          other_count = count

    if len(rule_id) > 0:
      cursor.execute(
        """INSERT INTO tweet_languages(en_count, in_count, other_count, rule_id) VALUES (%s, %s, %s, %s)""",
        (en_count, in_count, other_count, "rule_id_dummy")
        # (en_count, in_count, other_count, rule_id)
      )
  
  postgresql.close_connection_cursor(connection, cursor)