select distinct gender from gold.dim_customers;

select * from gold.dim_products dp ;

select * from gold.fact_sales t ;

-- check if all dimension tables can sucessfully join the fact table
select * from gold.fact_sales fc 
left join gold.dim_customers dc 
on dc.customer_key=fc.customer_key
where dc.customer_key is null;

select * from gold.fact_sales fc 
left join gold.dim_products dp 
on dp.product_key=fc.product_key
where dp.product_key is null;