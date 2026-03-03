select * from bronze.crm_cust_info where cst_id=29466;
-- Check the creation date to choose the newer one based on the create date

select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info where cst_id=29466;
-- Check the creation date to choose the newer one based on the create date

select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info;
-- Check the creation date to choose the newer one based on the create date

select * FROM( 
	select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info
) where flag_last!=1;
-- Check the creation date to choose the newer one based on the create date

select * FROM( 
	select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info
) where flag_last=1 and cst_id=29466;
-- Check the freshiest data from this primary key

-- Check for unwanted spaces in string columns, like name, trimming the end and the beggining
select cst_firstname
from bronze.crm_cust_info
where cst_firstname!=TRIM(cst_firstname);

-- Check for unwanted spaces in string columns, like name, trimming the end and the beggining
select cst_lastname
from bronze.crm_cust_info
where cst_lastname!=TRIM(cst_lastname);

-- Check for unwanted spaces in string columns, like name, trimming the end and the beggining
select cst_gndr
from bronze.crm_cust_info
where cst_gndr!=TRIM(cst_gndr);

select cst_id,cst_key,TRIM(cst_firstname) as cst_firstname,TRIM(cst_lastname) as cst_lastname,cst_marital_status,cst_gndr,cst_create_date FROM(
	select * FROM( 
			select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info where cst_id is not NULL
		) where flag_last=1
);

-- Data consistency and standardization
select distinct cst_gndr from bronze.crm_cust_info;

select cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_marital_status,
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

-- Data consistency and standardization
select distinct cst_marital_status st_marital_status from bronze.crm_cust_info;

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

-- The date column we have to make sure its the date type

-- Now, let's insert into silver crm_cust_info
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

-- Now, let's test the silver table

-- Check the primary key quality (no repeated values and nulls)!
select cst_id,COUNT(*) from silver.crm_cust_info group by cst_id having COUNT(*)>1 or cst_id is NULL;
-- Okay!

-- Check for unwanted spaces in string columns, like name, trimming the end and the beggining
select cst_firstname
from silver.crm_cust_info
where cst_firstname!=TRIM(cst_firstname);
-- Okay!

-- Check for unwanted spaces in string columns, like name, trimming the end and the beggining
select cst_lastname
from silver.crm_cust_info
where cst_lastname!=TRIM(cst_lastname);
-- Okay!

-- Check for unwanted spaces in string columns, like name, trimming the end and the beggining
select cst_gndr
from silver.crm_cust_info
where cst_gndr!=TRIM(cst_gndr);
-- Okay!

-- Data consistency and standardization
select distinct cst_gndr from silver.crm_cust_info;
-- Okay!

-- Data consistency and standardization
select distinct cst_marital_status from silver.crm_cust_info;
-- Okay!

select * from silver.crm_cust_info;


-- Removed unwanted spaces (trimming) done
-- Data Standardization/Normalization for Marital_Status and Gender done,
-- We handled missing data by adding a default value done,
-- We removed duplicates and filtered done, 


-- Now, let's go to the crm_prd_info
select * from bronze.crm_prd_info;
-- Check the primary key quality (no repeated values and nulls)!
select prd_id,COUNT(*) from bronze.crm_prd_info group by prd_id having COUNT(*)>1 or prd_id is NULL;
-- No duplicates for primary key

-- Now, the prd_key -> inside this column, the first 5 digits are a subcategory that needs to be created
select
prd_id,
prd_key,
SUBSTRING(prd_key,1,5) as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

select distinct id from bronze.erp_px_cat_g1v2;

-- no prd_info cat_id esta xx-xx, ja no px_cat_g1v2 esta xx_xx
select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

-- now we have to discover if there is a subcategory not found between two tables
select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where replace(SUBSTRING(prd_key,1,5),'-','_') not in (select distinct id from bronze.erp_px_cat_g1v2);

-- now we have to extract the rest of the column (the digit 6 all the way to the end)
select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

-- Now, check the salesdetails table to check the new column
select * from bronze.crm_sales_details;

select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,length(prd_key)) not in 
(select sls_prd_key from bronze.crm_sales_details);

-- Many products that don't have any orders
select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,length(prd_key)) not in 
(select sls_prd_key from bronze.crm_sales_details);

select sls_prd_key from bronze.crm_sales_details where sls_prd_key like 'FK-16%';
select sls_prd_key from bronze.crm_sales_details where sls_prd_key like 'FK%';
-- There are many rows in sales_details that a product key don't match with the product key of prd_info

-- To select only witch matches
-- Many products that don't have any orders
select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,length(prd_key)) in 
(select sls_prd_key from bronze.crm_sales_details);

-- check for unwanted spaces (trimming now)
select prd_nm from bronze.crm_prd_info where prd_nm!=trim(prd_nm);

-- okay!

-- Check for nulls and negative numbers
select prd_cost from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null;

select
prd_id,
prd_key,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
case when prd_cost is null then 0
	else prd_cost
end as prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;

-- Data standardization and consistency
select distinct prd_line from bronze.crm_prd_info;

select
prd_id,
prd_key,
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
prd_end_dt
from bronze.crm_prd_info;
 
-- check for dates
select * from bronze.crm_prd_info where prd_end_dt < prd_start_dt;
-- the data of the start is bigger than the end, this should be fixed
-- solution 1: change between start_dt and end_dt
-- but there is a problem, there is a overlap of price between two dates: the end of the first history should be 
-- lower than the start of the new history for the same product and each record must have a start date

-- solution 2: derive the end date from the start date -> the end date of the record is the start date of the next record and substract a day, for example
-- so, to test, it will do the suggested changes only in a few columns needed

select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt ASC)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');

select
prd_id,
prd_key,
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


-- Now, let's test the silver table

-- Check the primary key quality (no repeated values and nulls)!
select prd_id,COUNT(*) from silver.crm_prd_info group by prd_id having COUNT(*)>1 or prd_id is NULL;
-- Okay!

-- check for negative numbers and nulls
select prd_cost from silver.crm_prd_info where prd_cost <0 or prd_cost is null;


-- Data consistency and standardization
select distinct prd_line from silver.crm_prd_info;
-- Okay!

-- check for the order of the dates
select * from silver.crm_prd_info where prd_end_dt < prd_start_dt;
-- Okay!

select * from silver.crm_prd_info;

-- now for the sales details
select * from bronze.crm_sales_details;

-- check for trim
select * from bronze.crm_sales_details where sls_ord_num !=TRIM(sls_ord_num) or sls_prd_key !=TRIM(sls_prd_key);

-- check for not matching prd_keys between tables
select * from bronze.crm_sales_details where sls_prd_key not in (select prd_key from silver.crm_prd_info);

-- check for not matching cust_ids between tables
select * from bronze.crm_sales_details where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- the dates are stored as integers
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <0;

-- the dates are stored as integers
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0;


-- return nulls if two given values are equal
select
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 or length(sls_order_dt::text)!=8 or sls_order_dt>20500101;

select
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt>20500101 
or sls_order_dt < 19000101
or sls_order_dt <=0
or length(sls_order_dt::text)!=8;

select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt=0 or length(sls_order_dt::text)!=8 then null
	else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

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
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

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
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;

-- order dates should always be smaller of its due dates and shipping dates
select * from bronze.crm_sales_details where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- business rules: sales must be equal to quantity multiplied by the price and there should be no negatives, zeros and nulls
select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity*sls_price
or sls_sales is null
or sls_quantity is null
or sls_price is null
or sls_sales <=0
or sls_quantity <= 0
or sls_price <=0
order by sls_sales,sls_quantity,sls_price;

-- this scenario should be discussed with the experts that generated that data -> data issues will be 
-- fixed directly in the source system OR in the data warehouse

-- the rules for fixing in the data warehouse: 
-- if sales is negative, zero or null, derive it using quantity and price
-- if a price is zero or null, calculate it using sales and quantity
-- if a price is negative, convert it to a positive value

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price)
	then sls_quantity*abs(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity*sls_price
or sls_sales is null
or sls_quantity is null
or sls_price is null
or sls_sales <=0
or sls_quantity <= 0
or sls_price <=0
order by sls_sales,sls_quantity,sls_price;


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
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price)
	then sls_quantity*abs(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details;

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
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price)
	then sls_quantity*abs(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <=0 then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details;

-- do the checks like the other two tables

-- all the silver tables of the crm source system are done
-- now, going to the erp

select * from bronze.erp_cust_az12;
-- cid should match cst_id from crm and cid has a few digits more in the beggining of the row

select cid,bdate,gen from bronze.erp_cust_az12 where cid like '%AW00011000%';
-- a few rows of the erp_cust_az12 have three more digits than cst_id

select cid,
case when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid
end cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid 
end not in(select distinct cst_key from silver.crm_cust_info);

select
case when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid
end cid,
bdate,
gen
from bronze.erp_cust_az12;

-- identifying out of range bdates
select
case when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid
end cid,
bdate,
gen
from bronze.erp_cust_az12
where bdate<'1924-01-01' OR bdate>current_date;
-- there are many SUPER old dates and future new dates
-- you should report to the source system, handle as bad data, or replace as null...


select
case when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid
end cid,
case when bdate > current_date then null
	else bdate
end as bdate, 
gen
from bronze.erp_cust_az12;

select distinct gen from bronze.erp_cust_az12;

select distinct gen, 
case when upper(trim(gen)) in ('F','Female') then 'Female'
	when upper(trim(gen)) in ('M','Male') then 'Male' 
	else 'n/a'
end as gen
from bronze.erp_cust_az12;


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

-- do the checks like
select distinct gen from silver.erp_cust_az12;

-- now for erp_loc_a101
select * from bronze.erp_loc_a101;

select cid,
cntry
from bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info;

select
replace(cid,'-','') cid,
cntry
from bronze.erp_loc_a101;

select
replace(cid,'-','') cid,
cntry
from bronze.erp_loc_a101 where replace(cid,'-','') not in
(select cst_key from silver.crm_cust_info);

select
replace(cid,'-','') cid,
cntry
from bronze.erp_loc_a101 where cid not in
(select cst_key from silver.crm_cust_info);

select distinct cntry from bronze.erp_loc_a101 order by cntry;

select distinct
case when trim(cntry)= 'DE' then 'Germany'
	when trim(cntry) in ('US','USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
end as cntry
from bronze.erp_loc_a101
order by cntry;

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

select * from silver.erp_loc_a101;

select * from bronze.erp_px_cat_g1v2;

select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

-- the id is okay compared to prd

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

select distinct
cat
from bronze.erp_px_cat_g1v2;
select distinct
subcat
from bronze.erp_px_cat_g1v2;
select distinct
maintenance
from bronze.erp_px_cat_g1v2;

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


select * from silver.erp_px_cat_g1v2;
