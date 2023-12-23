# TOP_250_IMDB_MOVIES

This project's goal is to analyze TOP 250 movies ever created according to IMDB ranking and compare it with my 
personal movie preferences.

It consists of the input file containing TOP 250 IMDB movie titles, raw and staging S3 buckets, one of them linked to SFTP server, Postgres DB instance on RDS AWS as a serving layer and a Tableau report. 
Data ingestion is performed via python script app.py which acts as an entry point to the application and 
.sh script which splits the input file into 5 parts.

Pipeline takes the input file splitted in 5 parts and makes a call to OMDB API for each title 
creating enriched files and uploads them over SSH to SFTP server on AWS linked to S3 "raw" bucket.
From "raw" storage new files are transfered into the staging bucket and from there data is loaded 
into Postgres database instance which functions as a serving layer for reporting via Tableau.



