// Database to manage stage objects, fileformats etc.
CREATE OR REPLACE DATABASE manage_db;
    
// Publicly accessible staging area    
CREATE OR REPLACE STAGE manage_db.public.aws_stage
    url='s3://bucketsnowflakes3';

// Description of external stage
DESC STAGE manage_db.public.aws_stage; 

// List files in stage
LIST @manage_db.public.aws_stage;


CREATE OR REPLACE TABLE manage_db.public.orders (
    order_id VARCHAR(30),
    amount INT,
    profit INT,
    quantity INT,
    category VARCHAR(30),
    subcategory VARCHAR(30));

//Load data using copy command
COPY INTO manage_db.public.orders
    FROM @manage_db.public.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails.csv');

    
select * from manage_db.public.orders;

// Transforming using the SELECT statement
CREATE OR REPLACE TABLE manage_db.public.orders_tmp (
    order_id varchar(30),
    amount INT,
    profit INT,
    profitable_flag VARCHAR(30),
    category_substring VARCHAR(5)
    );

    
COPY INTO manage_db.public.orders_tmp (order_id, profit, profitable_flag, category_substring)
    FROM (select 
            s.$1,
            s.$3,
            CASE WHEN CAST(s.$3 as int) < 0 THEN 'not profitable' ELSE 'profitable' END ,
            substring(s.$5,1,5) 
          from @manage_db.public.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


select * from manage_db.public.orders_tmp;


drop table manage_db.public.orders_tmp
