create external table nation (
  nation_key int,
  name string,
  region_key int,
  comment_col string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location '/user/cloudera/nation';

create table nation_parquet like nation stored as parquetfile;

insert overwrite table nation_parquet select * from nation;
