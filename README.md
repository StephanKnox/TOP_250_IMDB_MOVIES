# TOP_250_IMDB_MOVIES

This project's goal is to analyze TOP 250 movies ever created according to IMDB ranking and compare it with my 
personal movie preferences.

It consists of the input file containing TOP 250 IMDB movie titles, raw and staging S3 buckets, one of them linked to SFTP server, Postgres DB instance on RDS AWS as a serving layer and a Tableau report. 
All data ingestion and orchestration is performed via a python script "app.py" which acts as an entry point to the application. 
Shell script "split_file.sh" splits the input file into 5 parts before making request to OMDB API.
In order not to overburden API interface requests are performed at a rate limit of 25 calls per minute.

Pipeline takes the input file splitted in 5 parts and makes a call to OMDB API for each title 
creating enriched files and uploads them over SSH to SFTP server on AWS linked to S3 "raw" bucket.
From "raw" storage new files are transfered into the staging bucket and from there data is loaded 
into Postgres database instance which functions as a serving layer for reporting via Tableau.

Tableau dashboard shows best movies per decade, genre, best years in cinematography quantified by me as when 
number of good movies produced in a year is 1.5 times higher than the average per year from 1920s up to 
present day. It also shows how my preferencescompare to IMDB rankings and what is the rating of movies I want 
to watch in the future.

<img width="799" alt="image" src="https://github.com/StephanKnox/TOP_250_IMDB_MOVIES/assets/123996543/4f6a9646-1c16-48f8-85bc-bbefcf813f20">




