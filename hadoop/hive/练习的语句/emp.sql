CREATE EXTERNAL TABLE IF NOT EXISTS employee_external ( name string,
work_place ARRAY<string>,
sex_age STRUCT<sex:string,age:int>,
skills_score MAP<string,int>,
depart_title MAP<STRING,ARRAY<STRING>> )
COMMENT 'This is an external table' ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/user/dayongd/employee';

create table ctas_employee as select * from employee_ecternal;

CREATE TABLE employee_partitioned(
name string,
work_place ARRAY<string>,
sex_age STRUCT<sex:string,age:int>, skills_score MAP<string,int>,
depart_title MAP<STRING,ARRAY<STRING>>
)
PARTITIONED BY (Year INT, Month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

alter table employee_partitioned ADD
partition (year=2014,month=11)
partition (year=2014,month=12)

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

MERGE INTO employee AS T
USING employee_state AS S
ON T.emp_id = S.emp_id and T.start_date = S.start_date
WHEN MATCHED AND S.state = 'update' THEN UPDATE SET dept_name = S.dept_name, work_loc = S.work_loc
WHEN MATCHED AND S.state = 'quit' THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (S.emp_id, S.emp_name, S.dept_name, S.work_loc, S.start_date);

CREATE table employee_hr(
 name string,
 employee_id int,
 sin_number string,
 start_date string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE

load data local inpath '/root/data/data/employee.txt' into table employee_external;

insert into table employee_partitioned partition(year,month)
SELECT
name,
ARRAY('Toronto') as work_place,
named_struct('sex','male','age',30) as sex_age,
map('python',90) as skill_score,
map('r&d',ARRAY('developer')) as depart_title,
year(start_date) as year,
month(start_date) as month
from employee_hr;

CREATE view view_employee_partitioned as SELECT * FROM employee_partitioned

-- lateral view explode(work_place) aaa as ww

select name,sa from employee_partitioned
lateral view skills_score employee_partitioned as sa;

load data inpath '/root/data/data/emp_basic.csv' overwrite into table emp_basic;

select eb.emp_id,eb.emp_name from emp_basic eb join emp_psn ep on eb.emp_id=ep.emp_id limit 5;
insert into table emp_basic select '1001',null,null,null,null,null from emp_basic limit 1;

-- drop table if exists emp_basic_b;
create dual emp_basic_d (
emp_id int, emp_name string, job_title string, company string, start_date date, quit_date date
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count"="0");



ALTER TABLE emp_basic CHANGE start_date start_date string;
ALTER TABLE emp_basic CHANGE quit_date quit_date string;

insert into table emp_basic values (1001,'emp_id'),('','emp_name'),('','job_title'),('','company'),('','start_date'),('','quit_date');

create table tt1 (
  id int,
  name string
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count"="1");


insert into table employee
SELECT
'testname',
ARRAY() as work_place,
named_struct('sex','male','age',30) as sex_age,
map('python',90) as skill_score,
map('r&d',ARRAY('developer')) as depart_title
from employee_hr limit 1;

CREATE TABLE employee(
name string,
work_place ARRAY<string>,
sex_age STRUCT<sex:string,age:int>, skills_score MAP<string,int>,
depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

















