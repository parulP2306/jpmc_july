create streaming table parul_silver.sales_cleaned_pl
(CONSTRAINT valid_order_id EXPECT (order_id is not null) on VIOLATION DROP ROW)
select distinct * from stream parul_bronze.sales_pipeline;

create or refresh streaming table parul_silver.products_cleaned;

CREATE FLOW product_flow
AS AUTO CDC INTO
  parul_silver.products_cleaned
FROM stream(parul_bronze.products_pipeline)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation, seqNum,_rescued_data)
  STORED AS SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE parul_silver.customers_cleaned;

CREATE FLOW customer_flow
AS AUTO CDC INTO
  parul_silver.customers_cleaned
FROM stream(parul_bronze.customers_pipeline)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum,_rescued_data)
  STORED AS SCD TYPE 2;