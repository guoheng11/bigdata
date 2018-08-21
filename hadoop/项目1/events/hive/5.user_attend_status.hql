-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- use database - events
USE events;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.tez.container.size=2048;
SET hive.tez.java.opts=-Xmx1536m;
SET mapred.child.java.opts="-server1g -XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit";

-- check if user_attend_status table exists
DROP TABLE IF EXISTS user_attend_status;
-- create user_attend_status table`
CREATE TABLE user_attend_status AS
    SELECT
        event_id, 
        attend_user_id, 
        MAX(invited) AS invited, 
        MAX(attended) as attended, 
        MAX(not_attended) AS not_attended, 
        MAX(maybe_attended) AS maybe_attended
    FROM event_users 
    GROUP BY event_id, attend_user_id;
		