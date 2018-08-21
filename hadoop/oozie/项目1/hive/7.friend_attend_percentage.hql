-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

-- check if friend_attend_percentage table exists
DROP TABLE IF EXISTS friend_attend_percentage;
-- create friend_attend_percentage table
CREATE TABLE friend_attend_percentage AS
    SELECT
        ecif.user_id, 
        ecif.event_id, 
        ecif.view_ahead_days, 
        ecif.event_creator_is_friend, 
        ecif.user_friend_count, 
        ecif.user_invited, 
        ecif.user_interested, 
	CASE WHEN fas.invited_friends_count IS NOT NULL THEN fas.invited_friends_count ELSE 0 END AS invited_friends_count, 
        CASE WHEN fas.attended_friends_count IS NOT NULL THEN fas.attended_friends_count ELSE 0 END AS attended_friends_count, 
        CASE WHEN fas.not_attended_friends_count IS NOT NULL THEN fas.not_attended_friends_count ELSE 0 END AS not_attended_friends_count, 
        CASE WHEN fas.maybe_attended_friends_count IS NOT NULL THEN fas.maybe_attended_friends_count ELSE 0 END AS maybe_attended_friends_count,
        CASE WHEN ecif.user_friend_count != 0 AND fas.invited_friends_count IS NOT NULL THEN fas.invited_friends_count * 100 / ecif.user_friend_count ELSE 0 END as invited_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.attended_friends_count IS NOT NULL THEN fas.attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as attended_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.not_attended_friends_count IS NOT NULL THEN fas.not_attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as not_attended_friends_percentage,		
        CASE WHEN ecif.user_friend_count != 0 AND fas.maybe_attended_friends_count IS NOT NULL THEN fas.maybe_attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as maybe_attended_friends_percentage
    FROM event_creator_is_friend ecif
        LEFT JOIN friend_attend_summary fas ON ecif.user_id = fas.user_id AND ecif.event_id = fas.event_id;

-- check if users table exists
DROP TABLE IF EXISTS users;
-- create users table
CREATE EXTERNAL TABLE users(user_id STRING, birth_year INT, gender STRING, locale STRING, location STRING, time_zone STRING, joined_at STRING) 
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, profile:birth_year, profile:gender, region:locale, region:location, region:time_zone, registration:joined_at')
    TBLPROPERTIES ('hbase.table.name' = 'users');
	
-- check if user_friend_event table exists
DROP TABLE IF EXISTS user_friend_event;
-- create user_friend_event table
CREATE TABLE user_friend_event AS
    SELECT
        fap.user_interested AS interested,
        fap.user_id, 
        fap.event_id, 
        u.locale,
        CASE WHEN u.gender = 'male' THEN 1 ELSE 0 END AS gender,
        2018 - u.birth_year AS age,
        fap.view_ahead_days,
        CASE WHEN fap.event_creator_is_friend IS NOT NULL THEN fap.event_creator_is_friend ELSE 0 END AS event_creator_is_friend,
        CASE WHEN fap.invited_friends_count IS NOT NULL THEN fap.invited_friends_count ELSE 0 END AS invited_friends_count,
        CASE WHEN fap.attended_friends_count IS NOT NULL THEN fap.attended_friends_count ELSE 0 END AS attended_friends_count,	
        CASE WHEN fap.not_attended_friends_count IS NOT NULL THEN fap.not_attended_friends_count ELSE 0 END AS not_attended_friends_count,
        CASE WHEN fap.maybe_attended_friends_count IS NOT NULL THEN fap.maybe_attended_friends_count ELSE 0 END AS maybe_attended_friends_count,
        CASE WHEN fap.invited_friends_percentage IS NOT NULL THEN fap.invited_friends_percentage ELSE 0 END AS invited_friends_percentage,		
        CASE WHEN fap.attended_friends_percentage IS NOT NULL THEN fap.attended_friends_percentage ELSE 0 END AS attended_friends_percentage,	
        CASE WHEN fap.not_attended_friends_percentage IS NOT NULL THEN fap.not_attended_friends_percentage ELSE 0 END AS not_attended_friends_percentage,
        CASE WHEN fap.maybe_attended_friends_percentage IS NOT NULL THEN fap.maybe_attended_friends_percentage ELSE 0 END AS maybe_attended_friends_percentage,
        fap.user_invited AS invited
    FROM friend_attend_percentage fap 
        INNER JOIN users u ON fap.user_id = u.user_id;