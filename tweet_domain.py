import postgresql

# Get tweet domains
def get_tweet_domains(obj):
  domains = []

  if hasattr(obj, 'data') and hasattr(obj.data, 'context_annotations') and len(obj.matching_rules) > 0:
    context_annotations = obj.data.context_annotations
    rule_id = str(obj.matching_rules[0].id)

    for context_annotation in context_annotations:
      domain = context_annotation.domain.name
      new_domain = domain.lower() + '|' + rule_id

      if new_domain not in domains:
        domains.append(new_domain)

  return domains

# Insert tweet domains into a table
def insert_tweet_domains(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_domains = rdd.take(100)

  for data in tweet_domains:
    parsed_domain = data[0].split("|")

    if len(parsed_domain) == 2:
      name = parsed_domain[0]
      count = data[1]
      rule_id = parsed_domain[1]

      cursor.execute(
        """INSERT INTO tweet_domains(name, count, rule_id) VALUES (%s, %s, %s)""",
        (name, count, rule_id)
      )
  
  postgresql.close_connection_cursor(connection, cursor)