set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;
set mapred.reduce.tasks = 256;

use userdb1;

create table user_temp(first_name string,last_name string,address varchar(20),country string,city string,state string,post varchar(20),phone1 varchar(20),phone2 varchar(20),email varchar(30),web varchar(30))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
stored as textfile;

load data local inpath '/home/cloudera/Desktop/UserRecords.txt' INTO table user_temp;

create table bucket_user(first_name string,last_name string,address varchar(20),country string,city string,state string,post varchar(20),phone1 varchar(20),phone2 varchar(20),email varchar(30),web varchar(30))

CLUSTERED BY(country) SORTED BY(state) INTO 25 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
stored as textfile;

insert into bucket_user
select first_name,last_name,address,city,post,phone1,phone2,email,web,country,state from user_temp;


