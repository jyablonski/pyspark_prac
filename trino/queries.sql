-- docker exec -it 57ee7234cedf trino
select count(*) from tpch.sf1.nation;

SHOW SCHEMAS FROM tpch;

```
information_schema 
 sf1                
 sf100              
 sf1000             
 sf10000            
 sf100000           
 sf300              
 sf3000             
 sf30000            
 tiny         
```

show tables in tpch.sf100000;

```
----------
 customer 
 lineitem 
 nation   
 orders   
 part     
 partsupp 
 region   
 supplier 
```

select * from tpch.sf100.customer limit 100;


select * from tpch.sf100000.supplier limit 1;

select max(acctbal) from tpch.sf100000.supplier limit 1;


select count(*) from tpch.sf300.supplier limit 1;




/* APACHE ICEBURGER QUERIES */
show tables in iceberg.nba_elt_iceberg;


select *
from iceberg.nba_elt_iceberg.pbp_data
limit 10;

select *
from iceberg.nba_elt_iceberg.reddit_comment_data
where scrape_ts >= CURRENT_DATE - INTERVAL '7' day;

-- creates the table in s3 bucket, adds it to the glue data catalog
create table if not exists iceberg.nba_elt_iceberg.reddit_comment_test as
select
	author,
	comment,
	score,
	url,
	scrape_ts
from iceberg.nba_elt_iceberg.reddit_comment_data
where scrape_ts >= CURRENT_DATE - INTERVAL '7' day;

drop table if exists iceberg.nba_elt_iceberg.reddit_comment_test;