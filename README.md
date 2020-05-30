## Sparkify Data Lake Project:
This is Project 5 in the Udacity Data Engineer Nanodegree.
It  creates a data-pipeline in AWS for the ficticious *Sparkify* online music streaming company.


### Data Sources
Data comes from two sources:
1) Song data is available from the [million song
data set](http://millionsongdataset.com/)
2) Event data is available from the [eventsim
github](https://github.com/Interana/eventsim). Eventsim is a data simulator for
user actions on a fictitious song streaming webpage.  Users can register for
the platform, listen to songs, change their memberships from free to paid and
vice versa and more.

Both data sets are stored together on [Udacity's own S3
bucket](s3://udacity-dend/) which is
publically read excessible.


### Data Pipeline
This data pipeline performs an ETL process that extracts song and log data from S3
and loads it in staging tables in a data warehouse in AWS redshift.  After
moving the raw data to staging tables in redshift it then transforms the data
into a star schema suitable for Online Analytical Processing (OLAP).

#### Data Pipeline diagram
![Data flows from S3 to staging tables in redshift, to a star schema in
redshift](./images/ETL_MAP.jpg "ETL Pipeline")

#### Data Star Schema
![songs and logs converted into star schema](./images/star_schema.png "Star
Schema ER diagram")


### DAG
![DAG diagram}(./images/dag_diagram.png)

### Run Instructions
Prerequisites:
  1) Create AWS redshift cluster
  2) Enable access to redshift cluster
  3) Create iam role with access key/secret key pair

Airflow:
  1) Install Airflow
  2) [Start airflow](https://airflow.apache.org/docs/stable/start.html)
  3) Create aws_credentials and redshift connections in airflow <TODO> add
  pictures from udacity

Redshift:
  1) Open Redshift query editor and run the sql statements in the
  `create_tables.sql` file to create the staging and star schema tables in
  redshift.
  2) copy dag to airflow dags location
  3) Open airflow and run the dag


