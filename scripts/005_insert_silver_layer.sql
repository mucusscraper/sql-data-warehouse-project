\set ON_ERROR_STOP on
\timing on
\echo '==============================';
\echo 'Loading the silver layer';
\echo '==============================';
BEGIN;
    \echo '-----------------------------------------';
    \echo 'Loading the silver layer for CRM tables';
    \echo '-----------------------------------------';
    truncate table silver.crm_cust_info;
    insert into silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)
    select cst_id,
    cst_key,
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
    case when UPPER(TRIM(cst_marital_status)) ='S' then 'Single'
	    when UPPER(TRIM(cst_marital_status))='M' then 'Married'
	    else 'n\a'
    end cst_marital_status,
    case when UPPER(TRIM(cst_gndr)) ='F' then 'Female'
	    when UPPER(TRIM(cst_gndr))='M' then 'Male'
	    else 'n\a'
    end cst_gndr,
    cst_create_date 
    FROM(
	    select * FROM( 
			select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info where cst_id is not NULL
		) where flag_last=1
    );
    truncate silver.crm_prd_info;
    insert into silver.crm_prd_info(
	    prd_id,
	    cat_id,
	    prd_key,
	    prd_nm,
	    prd_cost,
	    prd_line,
	    prd_start_dt,
	    prd_end_dt
    )
    select
    prd_id,
    replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
    prd_nm,
    case when prd_cost is null then 0
	    else prd_cost
    end as prd_cost,
    case when UPPER(TRIM(prd_line))='M' then 'Mountain'
	    when UPPER(TRIM(prd_line))='R' then 'Road'
	    when UPPER(TRIM(prd_line))='S' then 'other Sales'
	    when UPPER(TRIM(prd_line))='T' then 'Touring'
	    else 'n/a'
    end as prd_line,
    prd_start_dt,
    lead(prd_start_dt) over (partition by prd_key order by prd_start_dt ASC)-1 as prd_end_dt
    from bronze.crm_prd_info;
    truncate table silver.crm_sales_details;
    insert into silver.crm_sales_details(
        sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )
    select
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt=0 or length(sls_order_dt::text)!=8 then null
	    else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case when sls_ship_dt=0 or length(sls_ship_dt::text)!=8 then null
	    else cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    case when sls_due_dt=0 or length(sls_due_dt::text)!=8 then null
	    else cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,
    sls_quantity,
    case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price)
	    else sls_sales
    end as sls_sales,
    case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
	    else sls_price
    end as sls_price
    from bronze.crm_sales_details;
    \echo '-----------------------------------------';
    \echo 'Loading the bronze layer for ERP tables';
    \echo '-----------------------------------------';
    truncate table silver.erp_cust_az12;
    insert into silver.erp_cust_az12(
        cid,
        bdate,
        gen)
    select
    case when cid like 'NAS%' then substring(cid,4,length(cid))
	    else cid
    end cid,
    case when bdate > current_date then null
	    else bdate
    end as bdate, 
    case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
	    when upper(trim(gen)) in ('M','MALE') then 'Male' 
	    else 'n/a'
    end as gen
    from bronze.erp_cust_az12;
    truncate table silver.erp_loc_a101;
    insert into silver.erp_loc_a101(
        cid,
        cntry)
    select distinct
    case when trim(cntry)= 'DE' then 'Germany'
	    when trim(cntry) in ('US','USA') then 'United States'
	    when trim(cntry) = '' or cntry is null then 'n/a'
	    else trim(cntry)
    end as cntry,
    replace(cid,'-','') cid
    from bronze.erp_loc_a101;
    truncate table silver.erp_px_cat_g1v2;
    insert into silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance
    )
    select
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2;
COMMIT;
\echo '-----------------------------------------';
\echo 'SUCCESS: Bronze layer loaded successfully';
\echo '-----------------------------------------';