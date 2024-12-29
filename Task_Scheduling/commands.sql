-- use database
use snowpipe_demo;

-- create table
create or replace table completed_orders(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(30),
    order_date date
);

-- select table
select * from completed_orders;

-- Schedule a task to ingest data in completed_orders from snowpipe source table
create or replace task target_table_ingestion
warehouse = COMPUTE_WH
SCHEDULE = "USING CRON */2 * * * * UTC" -- every 2 minutes
AS
INSERT INTO completed_orders SELECT * FROM orders_data_lz where order_status = 'Completed';

-- By default, new task is created in a suspended state. You need to resume it to start its execution as per the defined schedule.
Alter task target_table_ingestion RESUME;

-- To suspend a task
alter task target_table_ingestion SUSPEND;

-- Check the history of task
Select * from table(information_schema.task_history(task_name=>'target_table_ingestion')) 
ORDER BY SCHEDULED_TIME DESC;

-- Drop a task
DROP TASK target_table_ingestion;

-- Example of chaining tasks
CREATE OR REPLACE TASK next_task
WAREHOUSE = COMPUTE_WH
AFTER target_table_ingestion
AS
DELETE FROM completed_orders
WHERE order_date < CURRENT_DATE();

ALTER TASK next_task RESUME;

SELECT CURRENT_TIMESTAMP();


SELECT * FROM table(information_schema.task_history(task_name=>'next_task')) ORDER BY SCHEDULED_TIME DESC;