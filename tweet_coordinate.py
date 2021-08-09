import postgresql

# Get tweet coordinates
def get_tweet_coordinates(obj):
  if hasattr(obj, 'data') and (hasattr(obj.data, 'geo')) and len(obj.matching_rules) > 0:
    rule_id = str(obj.matching_rules[0].id)
    
    if (hasattr(obj.data.geo, 'coordinates')):
      latitude = obj.data.geo.coordinates.coordinates[1]
      longitude = obj.data.geo.coordinates.coordinates[0]

      return [latitude, longitude, rule_id]

  return []

# Insert tweet coordinates into a table
def insert_tweet_coordinates(rdd):
  connection, cursor = postgresql.get_connection_cursor()
  tweet_coordinates = rdd.take(100)

  for data in tweet_coordinates:
    latitude = data[0]
    longitude = data[1]
    rule_id = data[2]

    cursor.execute(
      """INSERT INTO tweet_geolocations(lat, long, rule_id) VALUES (%s, %s, %s)""",
      (latitude, longitude, rule_id)
    )
  
  postgresql.close_connection_cursor(connection, cursor)