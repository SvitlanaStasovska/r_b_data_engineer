select 
COUNT(s.product_name) as cnt_TV,  
usr.state,
s.product_name
From `de-07-svitlana-stasovska.gold.user_profiles_enriched` as usr
Inner Join 
`de-07-svitlana-stasovska.Silver.Sales` as s ON usr.client_id = s.client_id
where date_diff(CURRENT_DATE(), usr.birth_date,  YEAR) between 20 and 30
and EXTRACT(MONTH  FROM s.purchase_date) = 9
and EXTRACT(DAY  FROM s.purchase_date) between 1 and 10
and s.product_name = 'TV'
group by usr.state, s.product_name
Order by cnt_TV
desc

---- Result: state = 'Iova', cnt_TV = 179