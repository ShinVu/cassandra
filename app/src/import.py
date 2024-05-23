from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import tuple_factory
from cassandra.cqltypes import FloatType
from dotenv import load_dotenv
import pandas as pd
import os
import numpy as np
# Load environment variables from the .env file
load_dotenv("../.env/.env")

# Retrieve the credentials from environment variables
username = os.getenv('CASSANDRA_USERNAME')
password = os.getenv('CASSANDRA_PASSWORD')
# Create an authentication provider instance
auth_provider = PlainTextAuthProvider(username=username, password=password)

#Default profile for execution
default_profile = ExecutionProfile(
    load_balancing_policy=WhiteListRoundRobinPolicy(['cassandra']),
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    request_timeout=15,
    row_factory=tuple_factory
)


# Initialize variables for the cluster and session
cluster = None
session = None

try:
    cluster = Cluster(['cassandra'],execution_profiles={EXEC_PROFILE_DEFAULT: default_profile}, auth_provider=auth_provider)
    session = cluster.connect("sparkifyks")
   
    # Specify the data type for each column
    dtype = {"userId": float, "sessionId": int, "itemInSession": int, "artist": str, "song": str, "length": float, "firstName": str, "lastName": str}
    
    # Read data to dataframe
    df = pd.read_csv("../data/event_data_merged.csv", sep=',', header = 0, dtype=dtype)
    
    # Replace Nan with none
    df = df.replace(np.nan, None)
    # Populate songinf_by_session_by_item table
    # Remove rows with null values for sessionId,itemInSession as Primary key can not be null
    df_cleaned_null_sessionId_itemInSession = df.dropna(axis=0,how='any',subset=['sessionId','itemInSession'])
    
    # INSERT rows to cassandra
    query = "INSERT INTO songinfo_by_session_by_item(sessionId,itemInSession,artist,song,length) VALUES (?,?,?,?,?)"
    prepared = session.prepare(query)
    for _,item in df_cleaned_null_sessionId_itemInSession.iterrows():
        session.execute(prepared, (item.sessionId,item.itemInSession,item.artist,item.song, item.length))
    
    # Populate songinfo_by_user_by_session table
    
    # Remove rows with null values for userId,sessionId,itemInSession as Primary key can not be null
    df_cleaned_null_userId_sessionId_itemInSession = df.copy()
    df_cleaned_null_userId_sessionId_itemInSession = df_cleaned_null_userId_sessionId_itemInSession.dropna(axis=0,how='any',subset=['userId','sessionId','itemInSession'])
    df_cleaned_null_userId_sessionId_itemInSession['userId'] = df_cleaned_null_userId_sessionId_itemInSession['userId'].astype(int)
    # INSERT rows to cassandra
    query = "INSERT INTO songinfo_by_user_by_session(userid,sessionid,iteminsession,artist,firstname,lastname,song) VALUES (?,?,?,?,?,?,?)"
    prepared = session.prepare(query)
    for _,item in df_cleaned_null_userId_sessionId_itemInSession.iterrows():
        session.execute(prepared, (item.userId,item.sessionId,item.itemInSession,item.artist,item.firstName,item.lastName,item.song))
    
except Exception as e:
    print(e)
finally:
    if session:
        session.shutdown()
    if cluster:
        cluster.shutdown()  