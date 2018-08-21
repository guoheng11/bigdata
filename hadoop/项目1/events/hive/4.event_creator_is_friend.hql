-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

-- check if event_creator_is_friend table exists
DROP TABLE IF EXISTS event_creator_is_friend;
-- create event_creator_is_friend table
CREATE TABLE event_creator_is_friend AS 
    SELECT 
        ue.user_id, 
        ue.event_id, 
        ue.user_invited, 
        ue.view_ahead_days, 
        ue.user_interested, 
        ue.event_creator, 
        CASE WHEN uf.friend_id IS NOT NULL THEN 1 ELSE 0 END AS event_creator_is_friend,
        CASE WHEN ufc.friend_count IS NOT NULL THEN ufc.friend_count ELSE 0 END AS user_friend_count
    FROM user_event ue 
        LEFT JOIN user_friend uf ON ue.user_id = uf.user_id AND ue.event_creator = uf.friend_id
        LEFT JOIN user_friend_count ufc ON ue.user_id = ufc.user_id;
