\set ON_ERROR_STOP on
\timing on
\echo '==============================';
\echo 'Loading the bronze layer';
\echo '==============================';
BEGIN;
    \echo '-----------------------------------------';
    \echo 'Loading the bronze layer for CRM tables';
    \echo '-----------------------------------------';
    truncate table bronze.crm_cust_info;
    \copy bronze.crm_cust_info FROM '/home/samsung/workspace/github.com/mucusscraper/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' DELIMITER ',' CSV HEADER;
    truncate table bronze.crm_prd_info;
    \copy bronze.crm_prd_info FROM '/home/samsung/workspace/github.com/mucusscraper/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' DELIMITER ',' CSV HEADER;
    truncate table bronze.crm_sales_details;
    \copy bronze.crm_sales_details FROM '/home/samsung/workspace/github.com/mucusscraper/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' DELIMITER ',' CSV HEADER;
    \echo '-----------------------------------------';
    \echo 'Loading the bronze layer for ERP tables';
    \echo '-----------------------------------------';
    truncate table bronze.erp_CUST_AZ12;
    \copy bronze.erp_CUST_AZ12 FROM '/home/samsung/workspace/github.com/mucusscraper/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
    truncate table bronze.erp_LOC_A101;
    \copy bronze.erp_LOC_A101 FROM '/home/samsung/workspace/github.com/mucusscraper/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
    truncate table bronze.erp_PX_CAT_G1V2;
    \copy bronze.erp_PX_CAT_G1V2 FROM '/home/samsung/workspace/github.com/mucusscraper/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
COMMIT;
\echo '-----------------------------------------';
\echo 'SUCCESS: Bronze layer loaded successfully';
\echo '-----------------------------------------';