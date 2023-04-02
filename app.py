import csv
import requests
import configparser
import json
import paramiko
from paramiko import RSAKey
import logging
import boto3
from botocore.exceptions import ClientError
import os
from datetime import timedelta
from ratelimit import limits, sleep_and_retry
import psycopg2


# Call to OMDB API with a ratelimit of 25 API calls per minute
@sleep_and_retry
@limits(calls=25, period=timedelta(seconds=60).total_seconds())
def get_movie_data(movie_omdb):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    api_omdb_key = parser.get("omdb_credentials", "omdb_api_key")
    omdb_url = parser.get("omdb_credentials", "omdb_url")

    q_params = {"t": movie_omdb, "r": "json", "apikey": api_omdb_key}
    resp = requests.get(omdb_url, params=q_params, timeout=10)

    if resp.ok:
        return resp
    return False


# Create files with data brought from OMDB API
def enrich_movies_file_from_omdb(movie_titles_files, dst_folder):
    try:
        for movie_titles_file in movie_titles_files:
            all_passes = []
            movie_titles = []

            export_filename = os.path.join(dst_folder, 'enriched_' + os.path.basename(movie_titles_file))

            with open(movie_titles_file, 'r') as data_file:
                csv_data = csv.reader(data_file)
                next(csv_data)

                for line in csv_data:
                    movie_titles.append(f"{line[0]}")
                for movie in movie_titles[:3]:
                    current_pass = []
                    resp = json.loads(get_movie_data(movie).content)
                    current_pass.append(resp['Title'])
                    current_pass.append(resp['Year'])
                    current_pass.append(resp['Rated'])
                    current_pass.append(resp['Released'])
                    current_pass.append(resp['Runtime'])
                    current_pass.append(resp['Genre'])
                    current_pass.append(resp['Director'])
                    current_pass.append(resp['Writer'])
                    current_pass.append(resp['Actors'])
                    current_pass.append(resp['imdbRating'])
                    all_passes.append(current_pass)

            with open(export_filename, 'w') as fp:
                csvw = csv.writer(fp, delimiter='|')
                csvw.writerows(all_passes)
            fp.close()

    except ClientError as e:
        logging.error(e)
        return False
    return True


def initialize_s3_client():
    """
    Initialize the s3 client
    """
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    s3_access_key = parser.get('aws_boto_credentials', 'access_key')
    s3_secret_key = parser.get('aws_boto_credentials', 'secret_key')
    region_name = parser.get('aws_boto_credentials', 'region_name')

    s3_client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )

    return s3_client


def initialize_sftp_client():
    """
    Initialize the sftp client
    """
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    sftp_hostname = parser.get("aws_sftp_server_credentials", "sftp_hostname")
    sftp_username = parser.get("aws_sftp_server_credentials", "sftp_username")
    sftp_private_key_filepath = parser.get("aws_sftp_server_credentials", "sftp_private_key_filepath")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key = RSAKey(filename=sftp_private_key_filepath)
    ssh.connect(
        hostname=sftp_hostname,
        username=sftp_username,
        pkey=key)
    sftp_client = ssh.open_sftp()

    return sftp_client


def list_s3_files_paginator(s3_client, bucket_name, s3_file_path):
    """
    Return S3 files for a specific bucket and prefix
    """
    s3_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_file_path)

    for page in pages:
        if page.get("KeyCount") > 0:
            for obj in page['Contents']:
                if 'enriched_' in obj['Key']:
                    s3_files.append(obj['Key'])
    return s3_files


def load_csv_to_postgres_aws_rds(s3_client, bucket, filepath, region):
    """
    Load contents of csv file to sql table on AWS RDS Instance
    """
    parser = configparser.ConfigParser()
    parser.read("./pipeline.conf")
    sql_host = parser.get("postgres_db_credentials", "host")
    sql_user = parser.get("postgres_db_credentials", "user")
    sql_password = parser.get("postgres_db_credentials", "password")
    sql_db = parser.get("postgres_db_credentials", "database")
    sql_table = parser.get("postgres_db_credentials", "table")

    files_to_load = list_s3_files_paginator(s3_client, bucket, filepath)

    sql_connection = psycopg2.connect(
        host=sql_host,
        user=sql_user,
        password=sql_password,
        database=sql_db
    )
    sql_cursor = sql_connection.cursor()

    for _file in files_to_load:
        # create load query
        load_query = """
                    SELECT aws_s3.table_import_from_s3(
                        '{sql_table}',
                        '', 
                        'DELIMITER ''|''',
                        aws_commons.create_s3_uri(
                            '{bucket}',
                            '{filepath}',
                            '{region}'
                        )
                    );""".format(sql_table=sql_table, bucket=bucket, filepath=_file, region=region)
        try:
            sql_cursor.execute(load_query)
            sql_connection.commit()
        except Exception as load_exception:
            sql_connection.rollback()
            raise Exception("SQL LOAD error: " + str(load_exception)) from load_exception

    sql_cursor.close()
    sql_connection.close()
    return True


def transfer_file_from_sftp_to_s3(sftp_client, s3_client, s3_src_bucket_filepath, s3_dst_bucket, s3_dst_filepath):
    try:
        sftp_client.chdir(s3_src_bucket_filepath)
        # List all items in the path
        file_list_all = sftp_client.listdir(".")
        file_list_relevant = [_file for _file in file_list_all if _file.startswith("enriched_")]

        for _file in file_list_relevant:
            with sftp_client.open(_file, "r") as f:
                f.prefetch()
                s3_client.put_object(
                    Bucket=s3_dst_bucket,
                    Key=s3_dst_filepath + os.path.basename(_file),
                    Body=f
                )
    except Exception as load_exception:
        raise Exception("Error occurred during SFTP file transfer to S3: " + str(load_exception)) from load_exception
    if sftp_client:
        sftp_client.close()
    return True


def upload_files_to_sftp(sftp_client, enriched_files, s3_dst_filepath):
    try:
        sftp_client.chdir(s3_dst_filepath)

        for _file in enriched_files:
            filename = os.path.basename(_file)
            sftp_client.put(_file, filename)

    except Exception as load_exception:
        raise Exception("Error occurred during SFTP file transfer: " + str(load_exception)) from load_exception
    if sftp_client:
        sftp_client.close()
    return True


def get_local_files_list(local_folder):
    files_list = []
    for root, directories, files in os.walk(local_folder):
        for _file in files:
            files_list.append(os.path.join(root, _file))
    return files_list


def main():
    """
    Main function which will take 5 source files containing movie titles, makes a call to OMDB API
    for each movie title creating enriched files and uploads them over SSH to SFTP server linked to S3 bucket
    serving as RAW storage.
    From there files are transferred to another S3 bucket serving as a staging bucket. From there files are
    loaded into Postgres database instance running on AWS RDS.
    """
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    src_local_folder = parser.get("data_folder", "src_folder_path")
    dst_local_folder = parser.get("data_folder", "dst_folder_path")
    s3_raw_bucket_folder = parser.get("aws_boto_credentials", "raw_bucket_filepath")
    s3_staging_bucket_name = parser.get("aws_boto_credentials", "staging_bucket_name")
    s3_staging_bucket_filepath = parser.get("aws_boto_credentials", "staging_bucket_filepath")
    s3_staging_bucket_region = parser.get("aws_boto_credentials", "region_name")

    # Get all files in the source folder into a list
    source_files = get_local_files_list(src_local_folder)
    # Pass this list and destination folder to a function which will make an API call for each record of each file
    # if API calls were a success and enriched files are created
    print("Starting API call...")
    if enrich_movies_file_from_omdb(source_files, dst_local_folder):
        out_files = get_local_files_list(dst_local_folder)
        # Provide a folder containing enriched files to a function which will send them to SFTP Server over SSH
        # linked to S3 bucket
        print("API call completed...")
        print("Starting SFTP upload...")
        sftp_client = initialize_sftp_client()
        if upload_files_to_sftp(sftp_client=sftp_client,
                                enriched_files=out_files,
                                s3_dst_filepath=s3_raw_bucket_folder):
            print("SFTP upload completed...")
            print("Starting file transfer from SFTP to S3 staging bucket...")
            s3_client = initialize_s3_client()
            sftp_client = initialize_sftp_client()
            if transfer_file_from_sftp_to_s3(sftp_client, s3_client, s3_raw_bucket_folder,
                                             s3_staging_bucket_name, s3_staging_bucket_filepath):
                print("File transfer from SFTP to S3 staging bucket completed...")
                print("Starting database load...")
                if load_csv_to_postgres_aws_rds(s3_client, s3_staging_bucket_name, s3_staging_bucket_filepath,
                                                s3_staging_bucket_region):
                    print("Database load completed...")


if __name__ == "__main__":
    main()
