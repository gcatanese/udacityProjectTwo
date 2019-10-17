# Project: Data Modeling with Cassandra

## Intro

The goal of the database is create a Cassandra data model able to allow Sparkify teams to perform specific queries
about user behaviour (which songs they play)


## Data Model

Given the requirements to produce 3 queries 3 different tables have been created:
* SONG_LIBRARY
* USER_PLAY
* SONG_HISTORY

The respective primary keys have been defined to ensure uniqueness of the values and to provide a suitable data partition.
A clustering column has been used in USER_PLAY to ensure the order of the query results

## Files

- Project_1B.ipynb: Jupiter notebook with all steps: create database, data tables and perform load/insert/query
- READMME.md: documentation

### Pre-requisites

1. Cassandra must run on localhost, default port
2. XLS file must be available in the /event_data folder







