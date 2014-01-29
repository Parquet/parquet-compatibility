create external table alltypes (
id int COMMENT 'Add a comment', 
bool_col boolean, 
tinyint_col tinyint, 
smallint_col smallint, 
int_col int, 
bigint_col bigint, 
float_col float, 
double_col double, 
date_string_col string, 
string_col string, 
timestamp_col timestamp)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location '/user/cloudera/alltypes';

create table alltypes_parquet like alltypes stored as parquetfile;

insert overwrite table alltypes_parquet select * from alltypes;

