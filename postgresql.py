import config
import psycopg2

# Get connection and cursor for PostgreSQL
def get_connection_cursor():
  connection = psycopg2.connect(
    user = config.db_user,
    password = config.db_password,
    host = config.db_host,
    port = config.db_port,
    database = config.db_name
  )
  cursor = connection.cursor()
  
  return connection, cursor

# Close connection and cursor for PostgreSQL
def close_connection_cursor(connection, cursor):
  connection.commit()
  cursor.close()
  connection.close()
