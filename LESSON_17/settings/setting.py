from pathlib import Path

folders = {
    'raw_dir_sales' : '/opt/data/sales' #Path('/opt/data/sales'),
    'raw_dir_customer' : '/opt/data/customer',
    'raw_dir_user_profile' : '/opt/data/user_profile')
}

gsc_settings =  {
             'ProjectId' : 'de-07-svitlana-stasovska' 
            ,'bucket_name' : 'gs://svt_bucket_02/'            
            }


query_sales_bronze_to_silver = """
INSERT INTO de-07-svitlana-stasovska.Silver.Sales_2(
    client_id,
    purchase_date,
    product_name,
    price,

    _id,
    _logical_dt,
    _job_start_dt
 )
 SELECT
    cast(CustomerId as INTEGER) AS CustomerId,
        COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y-%h-%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y.%m.%d', PurchaseDate)
    ) as purchase_date,
    Product,
    CAST(REGEXP_REPLACE(Price, r'USD|\$', '') as INTEGER) as price,
    GENERATE_UUID() AS _id,
    CAST(CURRENT_DATE AS DATE) AS _logical_dt,
    CAST(CURRENT_DATE AS DATE) AS _job_start_dt

FROM de-07-svitlana-stasovska.bronze.Sales
"""

query_customer_bronze_to_silver = """
INSERT INTO `de-07-svitlana-stasovska.Silver.Customer` (
    client_id, 
    first_name, 
    last_name, 
    email, 
    registration_date, 
    state,
    _id,
    _logical_dt,
    _job_start_dt
 )
 SELECT
    cast(Id as INTEGER) AS CustomerId,
    FirstName,
    LastName,
    Email,
        COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', RegistrationDate),
        SAFE.PARSE_DATE('%Y/%m/%d', RegistrationDate),
        SAFE.PARSE_DATE('%Y-%h-%d', RegistrationDate),
        SAFE.PARSE_DATE('%Y.%m.%d', RegistrationDate)
    ) as registration_date,
    State,
    GENERATE_UUID() AS _id,
    CAST(CURRENT_DATE AS Date) AS _logical_dt,
    CAST(CURRENT_DATE AS Date) AS _job_start_dt
FROM `de-07-svitlana-stasovska.bronze.Customer` 
"""

query_user_profile_bronze_to_silver = """INSERT INTO `de-07-svitlana-stasovska.Silver.user_profile` (
    email,
    full_name,
    state,
    birth_date,
    phone_number
 )
 SELECT
    email,
    full_name,
    state,
    birth_date,
    phone_number

FROM `de-07-svitlana-stasovska.bronze.user_profile`
;"""

query_user_profile_enriched ="""CREATE OR REPLACE TABLE `de-07-svitlana-stasovska.gold.user_profiles_enriched` AS                   
SELECT
    c.client_id,
    CASE WHEN c.first_name IS NULL OR c.first_name = '' THEN SPLIT(up.full_name, ' ')[OFFSET(0)] ELSE c.first_name END AS first_name,
    CASE WHEN c.last_name IS NULL OR c.last_name = '' THEN SPLIT(up.full_name, ' ')[OFFSET(1)] ELSE c.last_name END AS last_name,
    c.email,
    c.registration_date,
    CASE WHEN c.state IS NULL OR c.state = '' THEN up.state ELSE c.state END as state,
    up.birth_date,
    up.phone_number,
    GENERATE_UUID() AS _id,
    CURRENT_TIMESTAMP() AS _logical_dt,
    CURRENT_TIMESTAMP() AS _job_start_dt
FROM
    `de-07-svitlana-stasovska.Silver.Customer` AS c
LEFT JOIN
    `de-07-svitlana-stasovska.Silver.user_profile` AS up
ON
    c.email = up.email;"""
    
query_sales_bronze_to_silver_upd ="""MERGE INTO `de-07-svitlana-stasovska.Silver.Sales_2` as  t1 USING `de-07-svitlana-stasovska.bronze.Sales`as t2
ON t1.client_id = t2.CustomerId
WHEN MATCHED THEN
    UPDATE SET     
        t1.client_id=cast(t2.CustomerId as INTEGER),
        t1.purchase_date,
        product_name=cast(CustomerId as INTEGER) AS CustomerId,
                    COALESCE(
                        SAFE.PARSE_DATE('%Y-%m-%d', t2.PurchaseDate),
                        SAFE.PARSE_DATE('%Y/%m/%d', t2.PurchaseDate),
                        SAFE.PARSE_DATE('%Y-%h-%d', t2.PurchaseDate),
                        SAFE.PARSE_DATE('%Y.%m.%d', t2.PurchaseDate)
                        ) as purchase_date,
        t1.product_name = t2.Product,
        t1.price=CAST(REGEXP_REPLACE(t2.Price, r'USD|\$', '') as INTEGER)

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
    client_id,
    purchase_date,
    product_name,
    price,
    _id,
    _logical_dt,
    _job_start_dt
 )
 values(
    cast(CustomerId as INTEGER) AS CustomerId,
        COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y-%h-%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y.%m.%d', PurchaseDate)
    ) as purchase_date,
    Product,
    CAST(REGEXP_REPLACE(Price, r'USD|\$', '') as INTEGER) as price
    )
    ;"""

query_customer_bronze_to_silver_upd ="""MERGE INTO `de-07-svitlana-stasovska.Silver.Customer` as  t1 USING `de-07-svitlana-stasovska.bronze.Customer`as t2
ON t1.client_id = t2.Id
WHEN MATCHED THEN
    UPDATE SET 
        t1.email = t2.email ,
        t1.full_name = t2.full_name,
        t1.state = t2.state,
        t1.birth_date = t2.birth_date,
        t1.phone_number = t2.phone_number
WHEN NOT MATCHED BY TARGET THEN
    INSERT (email,
    full_name,
    state,
    birth_date,
    phone_number) VALUES (    
    t2.email,
    t2.full_name,
    t2.state,
    t2.birth_date,
    t2.phone_number)
    ;"""
