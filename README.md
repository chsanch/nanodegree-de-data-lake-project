# Data Lake (Udacity Nanodegree Project)

## Introduction A music streaming startup, Sparkify, has grown their user base
and song database and want to move their processes and data onto the cloud.
Their data resides in S3, in a directory of JSON logs on user activity on the
app, as well as a directory with JSON metadata on the songs in their app.

## Project ### Requirements
- Access to AWS EMR services
- Create a Spark Cluster
- Fill in the appropiate values in the `dl.cfg` file
- Change the value of `output_data` in `etl.py` with the name of your S3 bucket 

### ETL Pipeline One way to execute this script is:
- Upload the file to the EMR cluster, using the command: ``` scp -i dl.cfg
  etl.py your-ssh-key.pem cluster-user@your-aws-cluster:~/ ```
- Once the files have been uploaded, you need to connect to your cluster: ```
  ssh -i your-ssh-key.pem cluster-user@your-aws-cluster ```
- To execute the script in the cluster: ``` spark-submit etl.py ```

The script will perform the following:
- Get or create a Spark session on the cluster
- Load the song data from the _song_data_ folder in `input_data` S3 bucket
- Process the song data and create the tables: songs and artists
- Write Parquet files in the `output_data` for the tables songs and artists
- Load the log data from _log_data_ folder in the `input_data` S3 bucket
- Process the log data and create the tables: time users, time, songs and
  songplays 
- Write Parquet files in the `output_data` for the tables time users, time,
  songs and songplays 
