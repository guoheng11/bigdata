--创建events数据库
create database if not exists events;
--use events
use events;
--check if train table exists
drop table if exists train;
create external table train (
row_key string,user_id string,event_id string,invited string, time_stamp string,interested string)
stroed by 'org.apache.hadoop.hive.HBaseStorageHandler'
with
)
