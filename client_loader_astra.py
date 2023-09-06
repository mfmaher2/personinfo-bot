import csv
import numpy
import openai
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from dotenv import dotenv_values
from cassandra.query import SimpleStatement

# Description: This file will load the clients dataset into Astra DB

# parameters #########
config = dotenv_values('.env')
openai.api_key = config['OPENAI_API_KEY']
SECURE_CONNECT_BUNDLE_PATH = config['SECURE_CONNECT_BUNDLE_PATH']
ASTRA_CLIENT_ID = config['ASTRA_CLIENT_ID']
ASTRA_CLIENT_SECRET = config['ASTRA_CLIENT_SECRET']
ASTRA_KEYSPACE_NAME = config['ASTRA_KEYSPACE_NAME']
model_id = "text-embedding-ada-002"

# Open a connection to the Astra database
cloud_config = {
    'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH
}
auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# This function will load a CSV and insert values into the Astra database
# Input format:
# firstname, lastname, address1, city, state, zip
#
# Astra table columns:
# firstname, lastname, address1, city, state, zip


with open('resources/output-000001.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader) # skip header row
    query = SimpleStatement(f"INSERT INTO {ASTRA_KEYSPACE_NAME}.user_info (firstname, lastname, address1, city, state, zip, embedding) VALUES (%s, %s, %s, %s, %s, %s, %s)")

    for row in reader:
        firstname = row[0]
        lastname = row[1]
        address1 = row[2]
        city = row[4]
        state = row[5]
        zip = row[6]
        row2 = firstname + lastname + address1 + city + state + zip
        # print(row2)
        # Create embedding for client containing all the rows
        embedding = openai.Embedding.create(input=row2, model=model_id)['data'][0]['embedding']

        # Insert values into Astra database
        session.execute(query, (firstname, lastname, address1, city, state, zip, embedding))

## TODO
# failed to bind prepared statement on embedding type
# cassandra.InvalidRequest: Error from server: code=2200 [Invalid query] message="cannot parse '?' as hex bytes"