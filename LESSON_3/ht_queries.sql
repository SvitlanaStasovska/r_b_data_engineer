/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...

select fc.category_id, c."name", count(1) as count_films
from public.film f
inner join public.film_category fc on f.film_id = fc.film_id 
inner join public.category c on c.category_id = fc.category_id 
group by fc.category_id, c."name"
order by count_films
desc
;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...

select  f.rental_duration, a.last_name, a.first_name
from public.actor a 
inner join public.film_actor fa on fa.actor_id = a.actor_id 
inner join public.film f on f.film_id = fa.film_id 
group by f.rental_duration, a.last_name, a.first_name
order by f.rental_duration
desc
limit 10
;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...

select  c."name"
from public.film_category fc 
inner join public.category c on c.category_id = fc.category_id 
inner join public.film f on f.film_id = fc.film_id  
inner join public.payment p on p.rental_id  = f.film_id 
where f.rental_duration*p.amount >=
(
select max(f.rental_duration*p.amount)
From public.film f 
inner join public.payment p on p.rental_id  = f.film_id 
)
group by c."name"
;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...

select f.film_id, f.title , i.film_id 
from public.film f 
full join public.inventory i on i.film_id = f.film_id 
where i.film_id is null
order by f.title 
;

    ---OR--- 

select f.film_id, f.title , i.film_id 
from public.film f 
left join public.inventory i on i.film_id = f.film_id 
where i.film_id is null
order by f.title 
;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...

select category_id , "name", last_name, first_name, cnt_actor
from 
(
select c.category_id , c."name", a.last_name, a.first_name, f.film_id , f.title 
,count(c.category_id) over (partition by a.actor_id) as cnt_actor
from public.actor a 
inner join public.film_actor fa on fa.actor_id = a.actor_id 
inner join public.film f on f.film_id = fa.film_id 
inner join public.film_category fc on fc.film_id = f.film_id 
inner join public.category c on c.category_id = fc.category_id 
where c."name" = 'Children' 
) as "a"
group by category_id , "name", last_name, first_name, cnt_actor
order by cnt_actor
desc
limit 3
;