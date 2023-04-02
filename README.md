# TOP_250_IMDB_MOVIES
This script takes several source files containing movie titles, makes a call to OMDB API
for each movie title creating enriched files and uploads them over SSH to SFTP server linked to S3 bucket
serving as RAW storage.
From there files are transferred to another S3 bucket serving as a staging bucket. From there files are
loaded into Postgres database instance running on AWS RDS.
