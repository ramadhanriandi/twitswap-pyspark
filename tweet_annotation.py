import postgresql

# Get tweet annotations
def get_tweet_annotations(obj):
  annotations = []

  if hasattr(obj, 'data') and hasattr(obj.data, 'entities') and len(obj.matching_rules) > 0:
    entities = obj.data.entities
    rule_id = str(obj.matching_rules[0].id)

    if hasattr(entities, 'annotations'):
      annotation_entities = entities.annotations 

      for annotation_entity in annotation_entities:
        annotation = annotation_entity.normalized_text
        new_annotation = annotation.lower() + '|' + rule_id

        if annotation not in annotations:
          annotations.append(new_annotation)

  return annotations

# Insert tweet annotations into a table
def insert_tweet_annotations(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_annotations = rdd.take(100)

  for data in tweet_annotations:
    parsed_annotation = data[0].split("|")

    if len(parsed_annotation) == 2:
      name = parsed_annotation[0]
      count = data[1]
      rule_id = parsed_annotation[1]

      cursor.execute(
        """INSERT INTO tweet_annotations(name, count, rule_id) VALUES (%s, %s, %s)""",
        (name, count, rule_id)
      )
  
  postgresql.close_connection_cursor(connection, cursor)