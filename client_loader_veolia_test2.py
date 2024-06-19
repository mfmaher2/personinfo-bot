from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import datetime

# Replace with your Cassandra cluster settings
CASSANDRA_NODES = ['127.0.0.1']  # List of your Cassandra nodes IP addresses
CASSANDRA_USER = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'

# Set up a Cassandra connection
auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
cluster = Cluster(CASSANDRA_NODES, auth_provider=auth_provider)
session = cluster.connect('customer')  # Connect to your keyspace

# Define a function to insert data into your table
def insert_data(tag_id, data_quality, event_time, event_value):
    session.execute(
        """
        INSERT INTO cdc_test_insight_ts (tag_id, data_quality, event_time, event_value)
        VALUES (%s, %s, %s, %s)
        """,
        (tag_id, data_quality, event_time, event_value)
    )

# Use the function to insert some data
insert_data('12345', 99, datetime.datetime.now(), 123.45)

# Always remember to close the session and cluster connection when you are done
session.shutdown()
cluster.shutdown()
