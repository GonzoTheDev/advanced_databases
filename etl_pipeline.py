import pandas as pd
import MySQLdb  # or use another MySQL connector library

# Connect to the source MariaDB database
source_conn = MySQLdb.connect(host="your_host", user="your_user", passwd="your_password", db="MusicCompDB")
source_cursor = source_conn.cursor()

# Connect to the destination MariaDB database
destination_conn = MySQLdb.connect(host="your_host", user="your_user", passwd="your_password", db="YourNewDB")
destination_cursor = destination_conn.cursor()

# Extract data from the source VOTES table
source_query = "SELECT * FROM VOTES;"
votes_data = pd.read_sql_query(source_query, source_conn)

# Transform data as needed (you might need to join tables, aggregate, etc.)

# Example transformation: Create a new column for total votes
votes_data['TOTAL_VOTES'] = votes_data.groupby(['AGE_GROUPID', 'COUNTYID', 'EDYEAR'])['VOTE'].transform('sum')

# Load data into the destination VOTES_FACT table
votes_data.to_sql('VOTES_FACT', destination_conn, if_exists='replace', index=False)

# Close database connections
source_conn.close()
destination_conn.close()
