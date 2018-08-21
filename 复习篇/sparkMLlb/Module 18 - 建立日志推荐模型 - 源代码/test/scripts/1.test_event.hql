-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.auto.convert.join=false;

-- check if test table exists
DROP TABLE IF EXISTS test;
-- create test table
CREATE EXTERNAL TABLE test(row_key STRING, user_id STRING, event_id STRING, invited STRING, time_stamp STRING, interested STRING)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, eu:user, eu:event, eu:invited, eu:time_stamp, eu:interested')
    TBLPROPERTIES ('hbase.table.name' = 'test');

-- check if events table exists
DROP TABLE IF EXISTS events;
-- create events table
CREATE EXTERNAL TABLE events(event_id STRING, start_time STRING, city STRING, state STRING, zip STRING, country STRING, latitude FLOAT, longitude FLOAT, user_id STRING)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, schedule:start_time, location:city, location:state, location:zip, location:country, location:lat, location:lng, creator:user_id')
    TBLPROPERTIES ('hbase.table.name' = 'events');

-- check if user_event table exists
DROP TABLE IF EXISTS user_event;
-- create user_event table
CREATE TABLE user_event AS 
    SELECT 
        t.user_id, 
        t.event_id, 
        t.invited AS user_invited, 
        datediff(from_unixtime(unix_timestamp(CONCAT(SUBSTR(e.start_time, 1, 10), ' ', SUBSTR(e.start_time, 12, 8)), 'yyyy-MM-dd hh:mm:ss')), from_unixtime(unix_timestamp(CONCAT(SUBSTR(t.time_stamp, 1, 10), ' ', SUBSTR(t.time_stamp, 12, 8)), 'yyyy-MM-dd hh:mm:ss'))) AS view_ahead_days, 
        t.interested AS user_interested, 
        e.user_id AS event_creator 
    FROM test t INNER JOIN events e ON t.event_id = e.event_id 
    WHERE t.time_stamp regexp '^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}.*' AND e.start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z';

