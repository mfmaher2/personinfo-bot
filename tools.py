from langchain.tools import BaseTool
from langchain.text_splitter import CharacterTextSplitter

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import dotenv_values
import openai

import streamlit as st

### parameters #########
config = dotenv_values('.env')
openai.api_key = config['OPENAI_API_KEY']
SECURE_CONNECT_BUNDLE_PATH = config['SECURE_CONNECT_BUNDLE_PATH']
ASTRA_CLIENT_ID = config['ASTRA_CLIENT_ID']
ASTRA_CLIENT_SECRET = config['ASTRA_CLIENT_SECRET']
ASTRA_KEYSPACE_NAME = config['ASTRA_KEYSPACE_NAME']

# Open a connection to the Astra database
cloud_config = {
    'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH
}
auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

### Total User Reader Tool #########
class UsersReaderTool(BaseTool):
    name = "Total User Reader"
    description = "This tool will read the total users and return it so you can see it."

    def _run(self, firstname):
        query = f"SELECT firstname, lastname, address1, city, state, zip FROM {ASTRA_KEYSPACE_NAME}.user_info WHERE firstname = '{firstname}'"
        rows = session.execute(query)
        user_list = []
        for row in rows:
            user_list.append({f"firstname is {row.firstname}, lastname is {row.lastname}, address is {row.address1}, city is {row.city}, state is {row.state} , zip is {row.zip}"})
        return user_list

        return user_list

    def _arun(self, query: str):
        raise NotImplementedError("This tool does not support async")
