-- Use role
use role accountadmin;

-- Create database
create or replace database snowpipe_demo;

-- Create Schema
create schema target;

-- Create table
create or replace table orders_data_lz(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(20),
    order_date date
);

-- Create a cloud storage integration in Snowflake
-- Integration means creating config based secure access
create or replace storage integration gcs_bucket_read_int
    type = EXTERNAL_STAGE
    storage_provider = 'gcs'
    enabled = True
    storage_allowed_locations = ('gcs://snowpipe_raw_data_01/');

drop integration gcs_bucket_read_int;

-- Retrieve the cloud storage Service Account for your snowflake account
desc storage integration gcs_bucket_read_int;

-- Service Account info for storage integration
-- abcdefd@gcpuscentral1-1dfa.iam.gserviceaccount.com

-- Stage means reference to a specific external location where data will arrive
create or replace stage snowpipe_stage
    url = 'gcs://snowpipe_raw_data_01/'
    storage_integration = gcs_bucket_read_int;

-- Show stages
show stages;

list @snowpipe_stage;

-- Create Pub-Sub Topic and Subscription
-- gsutil notification create -t snowpipe_pubsub_topic -f json gs://snowpipe_raw_data/

-- create notification integration
create or replace notification integration notification_from_pubsub_int
    type = queue
    notification_provider = GCP_PUBSUB
    enabled = True
    gcp_pubsub_subscription_name = 'projects/abcdefd-123456/subscriptions/snowpipe_topic-sub';

-- Describe integration
desc integration notification_from_pubsub_int;

-- Service Account for Pub-Sub
-- abcdefd@gcpuscentral1-1dfa.iam.gserviceaccount.com

-- Create SnowPipe
create or replace pipe gcs_to_snowflake_pipe
auto_ingest = True
integration = notification_from_pubsub_int
as
copy into ORDERS_DATA_LZ
from @snowpipe_stage
file_format = (type = 'CSV');

-- Show pipes
show pipes;

-- Check the status of pipe
select system$pipe_status('gcs_to_snowflake_pipe');

-- Check the history of ingestion
select * from table(information_schema.copy_history(table_name=>'ORDERS_DATA_LZ',start_time=>dateadd(hours,-1,current_timestamp())));

select * from orders_data_lz;


















