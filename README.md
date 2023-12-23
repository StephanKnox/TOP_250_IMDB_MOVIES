# TOP_250_IMDB_MOVIES

This project's goal is to analyze TOP 250 movies ever created according to IMDB ranking and compare it with my 
personal movie preferences.

It consists of the input file containing TOP 250 IMDB movie titles, raw and staging storage buckets on AWS

.sh script which splits this file into 5 parts,
app.py script serving as an entry point to the application

This script takes several source files  makes a call to OMDB API
for each title creating enriched files and uploads them over SSH to SFTP server on AWS linked to S3 bucket which
serves as a RAW storage.

From there files are transferred to another S3 bucket serving as staging storage. Then files are
loaded into Postgres database instance running on AWS RDS which functions as a serving layer for reporting
via Tableau.

