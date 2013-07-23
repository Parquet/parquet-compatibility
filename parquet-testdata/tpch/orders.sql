create external table orders (
  order_key bigint,
  cust_key bigint,
  order_status string,
  total_price double,
  order_date string,
  order_priority string,
  clerk string,
  ship_priority int,
  comment_col string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location '/user/cloudera/orders';

create table orders_parquet like orders stored as parquetfile;

insert overwrite table orders_parquet select * from orders;
