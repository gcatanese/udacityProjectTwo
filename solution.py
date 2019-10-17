# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


print("Starts...")

# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)

# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []

# for every filepath in the file path list
for f in file_path_list:

    # reading csv file
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)

        # extracting each data row one by one and append it
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line)

        # uncomment the code below if you would like to get total number of rows
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length', \
                     'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

# This should make a connection to a Cassandra instance your local machine
# (127.0.0.1)

from cassandra.cluster import Cluster

try:
    cluster = Cluster()

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
except Exception as e:
    print(e)

try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """)

except Exception as e:
    print(e)

try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "CREATE TABLE IF NOT EXISTS song_library "
query = query + "(session_id int, item_in_session int, artist_name text, song_title text, song_length double, PRIMARY KEY (session_id, item_in_session))"

session.execute(query)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO song_library (session_id, item_in_session, artist_name, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "SELECT artist_name, song_title, song_length FROM song_library WHERE session_id=411 AND item_in_session = 4"
rows = session.execute(query)

for row in rows:
    print (row)


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query = "CREATE TABLE IF NOT EXISTS user_play "
query = query + "(user_id int, session_id int, item_in_session int, artist_name text, song_title text, first_name text, " \
                "last_name text, PRIMARY KEY ((user_id, session_id), item_in_session))"
session.execute(query)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO user_play (user_id, session_id, item_in_session, artist_name, song_title, " \
                "first_name, last_name)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "SELECT artist_name, song_title, first_name, last_name FROM user_play WHERE user_id=10 AND session_id=182"
rows = session.execute(query)

for row in rows:
    print (row)

## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "CREATE TABLE IF NOT EXISTS song_history "
query = query + "(song_title text, first_name text, last_name text, PRIMARY KEY (song_title, first_name, last_name))"
session.execute(query)

# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO song_history (song_title, first_name, last_name)"
        query = query + "VALUES (%s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[9], line[1], line[4]))

## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "SELECT first_name, last_name FROM song_history WHERE song_title='All Hands Against His Own'"
rows = session.execute(query)

for row in rows:
    print (row)

query = "DROP TABLE song_library"
session.execute(query)

query = "DROP TABLE user_play"
session.execute(query)

query = "DROP TABLE song_history"
session.execute(query)


session.shutdown()
cluster.shutdown()

print("Pipeline is completed")