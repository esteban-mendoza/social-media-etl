# ETL Challenge

By using the following API:

Base URL: <https://jsonplaceholder.typicode.com>

    GET   /users
    GET   /posts
    GET   /posts/1
    GET   /posts/1/comments
    GET   /comments?postId=1

A usage example could be: `GET https://jsonplaceholder.typicode.com/posts`

1. Create the SQL statements to create the tables of a database that will store the data from the API, it can be on any Relational Database (MySQL, Postgres or any other)

2. By using a Python script, read the data from the API endpoints, do neccesary transformations and load it into the database, you can use any framework or library (pandas, pyspark or any other)

3. Write the sql statements for:

<ol type="a">
    <li>Retrieve the userId that made most comments on all the Posts</li>
    <li>Retrieve the number of comments per Post</li>
    <li>Retrieve the longest post comment made on all the posts</li>
</ol>

The deliverables should be the followings:

1. On a file named as "blog_post.sql", will be the SQL statements of 1.
2. On a file named as "blog_post_etl.py", will be the script code of 2.
3. On a file named as "blog_post_queries.sql", will be the queries of 3.
