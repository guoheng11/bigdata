-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.auto.convert.join=false;

-- check if friend_attend_status table exists
DROP TABLE IF EXISTS friend_attend_status;
-- create friend_attend_status table
CREATE TABLE friend_attend_status AS
    SELECT
        uf.user_id, 
        uf.friend_id, 
        uas.event_id, 
		CASE WHEN uas.invited IS NOT NULL AND uas.invited > 0 THEN 1 ELSE 0 END AS invited, 
        CASE WHEN uas.attended IS NOT NULL AND uas.attended > 0 THEN 1 ELSE 0 END AS attended, 
        CASE WHEN uas.not_attended IS NOT NULL AND uas.not_attended > 0 THEN 1 ELSE 0 END AS not_attended, 
        CASE WHEN uas.maybe_attended IS NOT NULL AND uas.maybe_attended > 0 THEN 1 ELSE 0 END AS maybe_attended
    FROM user_friend uf 
        LEFT JOIN user_attend_status uas ON uf.friend_id = uas.attend_user_id;
		
-- check if friend_attend_summary table exists
DROP TABLE IF EXISTS friend_attend_summary;
-- create friend_attend_summary table
CREATE TABLE friend_attend_summary AS
    SELECT 
        user_id, event_id, 
        SUM(invited) AS invited_friends_count, 
        SUM(attended) AS attended_friends_count, 
        SUM(not_attended) AS not_attended_friends_count, 
        SUM(maybe_attended) AS maybe_attended_friends_count
    FROM friend_attend_status 
    WHERE event_id IS NOT NULL
    GROUP BY user_id, event_id;	