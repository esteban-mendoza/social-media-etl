
-- a) Retrieve the userId that made most comments on all the Posts

with count_by_email as (
	select count(*) as comment_count
	from "comments" c 
	group by email 
), 
emails_most_comments as (
	select email
	from "comments" c
	group by email
	having count(*) = (
		select max(comment_count)
		from count_by_email
	) 
)
select u.id as user_id, e.email
from users u 
right join emails_most_comments e
on u.email = e.email;


-- b) Retrieve the number of comments per Post

SELECT post_id, count(*)
FROM "comments" c
GROUP BY post_id
ORDER BY post_id;

-- c) Retrieve the longest post comment made on all the posts

SELECT body, length(body)
FROM "comments" c
WHERE length(body) = (
    SELECT max(length(body))
    FROM "comments" c
);
