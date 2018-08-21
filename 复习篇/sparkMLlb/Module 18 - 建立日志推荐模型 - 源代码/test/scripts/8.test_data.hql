-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.auto.convert.join=false;

-- check if friend_attend_percentage table exists
DROP TABLE IF EXISTS locale;
-- create locale table
CREATE EXTERNAL TABLE locale
(
    locale_id INT,
	locale STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/events/locale';

-- check if user_friend_event table exists
DROP TABLE IF EXISTS test_data;
-- create test_data table
CREATE TABLE test_data 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
AS
    SELECT
        ufe.interested,
        ufe.user_id, 
        ufe.event_id, 
        CASE WHEN l.locale_id IS NOT NULL THEN l.locale_id ELSE 0 END AS locale_id,
        ufe.gender,
        ufe.age,
        ufe.view_ahead_days,
        ufe.event_creator_is_friend,
        ufe.invited_friends_count,
        ufe.invited_friends_percentage,		
        ufe.attended_friends_percentage,	
        ufe.not_attended_friends_percentage,
        ufe.maybe_attended_friends_percentage,
        ufe.invited
    FROM user_friend_event ufe 
        LEFT JOIN locale l ON ufe.locale = l.locale;
