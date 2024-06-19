import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
import sys
import numpy as np

def main():
    try:
        # SSL configuration
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        # Path to your CA certificate file
        ssl_context.load_verify_locations('resources/ca-bundle.pem')
        ssl_context.verify_mode = CERT_REQUIRED

        # Path to your certificate and private key files
        certificate_file = 'resources/client.example.com.pem'
        private_key_file = 'resources/client.example.com.key'

        # Load the certificate and private key
        ssl_context.load_cert_chain(certfile=certificate_file, keyfile=private_key_file)

        # Cassandra connection details
        cassandra_host = '34.122.148.2'
        cassandra_port = 443
        username = 'superuser'
        password = ''
        keyspace = 'tech360'
        table = 'tech360_fwa_poc_c'

        # Authentication provider
        auth_provider = PlainTextAuthProvider(username=username, password=password)

        # Create cluster instance with SSL options
        cluster = Cluster(
            contact_points=[cassandra_host],
            port=cassandra_port,
            auth_provider=auth_provider,
            ssl_context=ssl_context
        )

        # Connect to the cluster
        session = cluster.connect()

        # Use the keyspace
        session.set_keyspace(keyspace)

        # Prepare the insert statement
        insert_stmt = session.prepare(f"""
            INSERT INTO {table} (row_id, attributes_blob, body_blob, metadata_s, vector)
            VALUES (?, ?, ?, ?, ?)
        """)

        # Path to the JSON file
        json_file_path = 'resources/json_schema.json'

        # Read the JSON file
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        # Extract data from JSON
        row_id = json_data['id']
        attributes_blob = json.dumps(json_data['metadata'])  # Convert metadata to JSON string
        body_blob = json_data['page_content']
        metadata_s = {k: str(v) for k, v in json_data['metadata'].items()}  # Ensure metadata values are strings

        # Generate a random vector with non-zero values for demonstration purposes
        vector = np.random.rand(768).tolist()

        # Bind the values to the prepared statement
        bound_stmt = insert_stmt.bind((
            row_id,
            attributes_blob,
            body_blob,
            metadata_s,
            vector
        ))

        # Execute the insert statement
        session.execute(bound_stmt)

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
    finally:
        # Ensure resources are properly closed
        if 'session' in locals():
            session.shutdown()
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    main()
