-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

-- check if event_attendee table exists
DROP TABLE IF EXISTS event_attendee;
-- create event_attendee table
CREATE EXTERNAL TABLE event_attendee(row_key STRING, event_id STRING, user_id STRING, attend_type STRING)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, euat:event_id, euat:user_id, euat:attend_type')
    TBLPROPERTIES ('hbase.table.name' = 'event_attendee');

-- check if event_users table exists
DROP TABLE IF EXISTS event_users;
-- create event_users table
CREATE TABLE event_users AS 
    SELECT 
        event_id, 
        user_id AS attend_user_id,
        CASE WHEN attend_type = 'invited' THEN 1 ELSE 0 END AS invited,
        CASE WHEN attend_type = 'yes' THEN 1 ELSE 0 END AS attended,
        CASE WHEN attend_type = 'no' THEN 1 ELSE 0 END AS not_attended,
        CASE WHEN attend_type = 'maybe' THEN 1 ELSE 0 END AS maybe_attended
    FROM event_attendee; 
