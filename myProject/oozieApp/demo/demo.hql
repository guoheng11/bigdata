-- CREATE DATABASE IF NOT EXISTS DEMO;
create database if not exists demo;
use demo;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;
set hive.auto.convert.join=false;
DROP TABLE IF EXISTS employee;
CREATE EXTERNAL TABLE employee(account STRING, firstName STRING, lastName String, department STRING, emailAddress STRING, phone STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, profile:firstName, profile:lastName, department:name, contact:emailAddress, contact:phone')
TBLPROPERTIES ('hbase.table.name' = 'employee');
DROP TABLE IF EXISTS users;
CREATE TABLE users AS SELECT * FROM employee;