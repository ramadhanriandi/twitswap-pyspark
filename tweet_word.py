import postgresql
import stop_words

# Get tweet words
def get_tweet_words(obj):
  words = []

  if hasattr(obj, 'data') and hasattr(obj.data, 'text') and hasattr(obj.data, 'lang') and len(obj.matching_rules) > 0:
    text = obj.data.text
    parsed_words = text.split(" ")
    lang = obj.data.lang
    rule_id = str(obj.matching_rules[0].id)

    for word in parsed_words:
      cleaned_word = word.lower()
      cleaned_word = cleaned_word.rstrip('.')
      cleaned_word = cleaned_word.rstrip(',')
      cleaned_word = cleaned_word.rstrip(';')
      cleaned_word = cleaned_word.rstrip(':')

      is_eligible = True
      
      if cleaned_word == "rt" or len(cleaned_word) < 2 or cleaned_word.startswith("@") or cleaned_word.startswith("&"):
        is_eligible = False
      elif lang == "en" and cleaned_word in stop_words.english:
        is_eligible = False
      elif lang == "in" and cleaned_word in stop_words.indonesia:
        is_eligible = False

      if is_eligible:
        new_word = cleaned_word + '|' + rule_id

        if new_word not in words:
          words.append(new_word)

  return words

# Insert tweet words into a table
def insert_tweet_words(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_words = rdd.take(100)

  for data in tweet_words:
    parsed_word = data[0].split("|")

    name = parsed_word[0]
    count = data[1]
    rule_id = parsed_word[1]

    cursor.execute(
      """INSERT INTO tweet_words(name, count, rule_id) VALUES (%s, %s, %s)""",
      (name, count, rule_id)
    )
  
  postgresql.close_connection_cursor(connection, cursor)