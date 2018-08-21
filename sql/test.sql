select restaurant_id,sum(score) as sum_score from (select score,restaurant_id from restaurant
LATERAL VIEW explode(grades.score) a AS score) group by restaurant_id

select sum(score) as sum_score,restaurant_id,cuisine from restaurant
LATERAL VIEW explode(grades.score) a AS score group by restaurant_id,cuisine having  sum_score<100 and sum_score>80 and cuisine!='American'
