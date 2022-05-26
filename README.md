# big-data-processing-hw4
Forth Homework for the UCU Big Data Processing course.
Results and screenshots are in results.pdf

##Usage:

```
bash run-cluster.sh
bash keyspace-tables.sh
bash write.sh
bash read.sh
bash shutdown-cluster.sh
```


To use queries after running flask app go to:

1. localhost:5000/queries?query=1&product_id=0140291784
2. localhost:5000/queries?query=2&product_id=0140291784&star_rating=3
3. localhost:5000/queries?query=3&customer_id=52837740
4. localhost:5000/queries?query=4&start=2001-01-01&end=2022-01-011&n=10
5. localhost:5000/queries?query=5&start=2001-01-01&end=2022-01-01&n=10
6. localhost:5000/queries?query=6&start=2001-01-01&end=2022-01-01&n=10
7. localhost:5000/queries?query=7&start=2001-01-01&end=2022-01-01&n=10
