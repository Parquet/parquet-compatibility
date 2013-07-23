-- assume customer.csv is copied to /user/cloudera/customer
-- 1.0 of impala doesn't support load data local inpath
-- ToDo change this to use load data local inpath for imapala 1.1
create external table customer (
  cust_key bigint,
  name string,
  address string,
  nation_key int,
  phone string,
  acctbal double,
  mktsegment string,
  comment_col string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location '/user/cloudera/customer';

create table customer_parquet like customer stored as parquetfile;

insert overwrite table customer_parquet select * from customer;
