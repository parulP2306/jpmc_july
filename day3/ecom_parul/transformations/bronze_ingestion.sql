--ingesting sales to bronze
create streaming table parul_bronze.sales_pipeline as
select * from stream read_files("s3://jpmctraining/input_files/sales", format=>"csv");

--ingesting products to bronze
create streaming table parul_bronze.products_pipeline as
select * from stream read_files("s3://jpmctraining/input_files/products", format=>"csv");

--ingesting customers to bronze
create streaming table parul_bronze.customers_pipeline as
select * from stream read_files("s3://jpmctraining/input_files/customers", format=>"csv");