-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

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
LOCATION '/user/events/';

-- check if user_friend_event table exists
DROP TABLE IF EXISTS train_data;
-- create train_data table
CREATE TABLE train_data AS
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
        INNER JOIN locale l ON ufe.locale = l.locale;

-- remove the existing folder		
dfs -rm -r '/user/events/train';
-- export
EXPORT TABLE train_data TO '/user/events/train/';

-- remove the existing folder		
-- dfs -rm -r $outputdir
-- export to the target folder
-- EXPORT TABLE train_data TO $outputdir;
