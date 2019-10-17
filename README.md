# Project: Data Modeling with Cassandra

## Intro

The goal of the database is create a Cassandra data model able to allow Sparkify teams to perform specific queries
about user behaviour (which songs they play).


## Data Model

Given the requirements to produce 3 queries 3 different tables have been created:
* SONG_LIBRARY
* USER_PLAY
* SONG_HISTORY

The respective primary keys have been defined to ensure uniqueness of the values and to provide a suitable data partition.
A clustering column has been used in USER_PLAY to ensure the order of the query results.

## Tasks

### Task 1: Getting the artist, song's title and song's length for a given session_id and item_in_session

In order to model this query the primary key includes session_id and itemInSession columns because the combination
is unique across all rows and we want keep the data in the same partition (partition key).

```
query = "CREATE TABLE IF NOT EXISTS song_library "
query = query + "(session_id int, item_in_session int, artist_name text, song_title text, song_length double, " \
                "PRIMARY KEY (session_id, item_in_session))"
```

### Task 2: Getting the artist, song's title and user for a given user_id and session_id, sorting by item_in_session

In order to model this query the primary key includes user_id and session_id because the combination
is unique across all rows.
The item_in_session is a clustering columng to ensure the query results are ordered according to that column.

```
query = "CREATE TABLE IF NOT EXISTS user_play "
query = query + "(user_id int, session_id int, item_in_session int, artist_name text, song_title text, " \
                "first_name text, last_name text, PRIMARY KEY ((user_id, session_id), item_in_session))"
```

### Task 3: Getting the users who listened to a given song

In order to model this query the primary key includes song's title, user first name, last name and id because
the combination is unique across all rows (it must consider the possibility of users with the same
first name and lastname, hence the need to include the user_id column).

```
query = "CREATE TABLE IF NOT EXISTS song_history "
query = query + "(song_title text, first_name text, last_name text, user_id int, " \
                "PRIMARY KEY (song_title, first_name, last_name, user_id))"
```

## Files

- Project_1B.ipynb: Jupiter notebook with all steps: create database, data tables and perform load/insert/query
- READMME.md: documentation

### Pre-requisites

1. Cassandra must run on localhost, default port
2. XLS file must be available in the /event_data folder







