CREATE KEYSPACE my_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE my_keyspace;
CREATE TABLE reviews (review_id text, product_id text, customer_id text, star_rating int, review_date date, review_headline text, PRIMARY KEY (product_id, star_rating, review_id));
CREATE TABLE customer_reviews (customer_id text, review_id text, product_id text, star_rating int, verified_purchase text, review_date date, review_headline text, PRIMARY KEY (customer_id, star_rating));
EXIT;
