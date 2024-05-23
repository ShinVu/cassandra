CREATE KEYSPACE IF NOT EXISTS sparkifyks 
WITH REPLICATION = 
{'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS sparkifyks.songinfo_by_session_by_item (    
sessionId INT,
itemInSession INT,
artist TEXT,
song TEXT,
length FLOAT,
PRIMARY KEY (sessionId, itemInSession));

CREATE TABLE IF NOT EXISTS sparkifyks.songinfo_by_user_by_session (
userId INT,
sessionId INT,
itemInSession INT,
artist TEXT,
song TEXT,
firstName TEXT,
lastName TEXT,
PRIMARY KEY ((userId, sessionId), itemInSession));