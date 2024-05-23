from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv("../.env/.env")

# Retrieve the credentials from environment variables
username = os.getenv('CASSANDRA_USERNAME')
password = os.getenv('CASSANDRA_PASSWORD')
# Create an authentication provider instance
auth_provider = PlainTextAuthProvider(username=username, password=password)

# Initialize variables for the cluster and session
cluster = None
session = None

try:
    cluster = Cluster(['cassandra'], auth_provider=auth_provider)
    session = cluster.connect()
except Exception as e:
    print(e)
finally:
    if session:
        session.shutdown()
    if cluster:
        cluster.shutdown()