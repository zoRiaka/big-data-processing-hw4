import pandas as pd
from datetime import datetime
import gzip
import shutil
import csv
import numpy as np

def create_new_csv(filename):
    with gzip.open(filename, 'rb') as f_in:
        with open('amazon_reviews.tsv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    with open("amazon_reviews.tsv") as file:
        # Passing the TSV file to
        # reader() function
        # with tab delimiter
        # This function will
        # read data from file
        tsv_file = csv.reader(file, delimiter="\t")

        i = 0
        # printing data line by line
        for line in tsv_file:
            print(line)
            i += 1
            if i > 10:
                break


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_reviews(self):
        col_list_reviews = ['review_id', 'product_id', 'customer_id', 'star_rating', 'review_date', 'review_headline']
        df_reviews = pd.read_csv("amazon_reviews.tsv", delimiter='\t',
                                 usecols=col_list_reviews,
                                 nrows=620000)  # the full number of rows is 3105371, we take 20%, so it will be around 620000
        df_reviews['review_date'].fillna('1000-01-01', inplace=True)
        df_reviews['star_rating'].fillna(0, inplace=True)
        df_reviews.fillna('-', inplace=True)
        query = "INSERT INTO reviews(review_id,product_id,customer_id,star_rating,review_date,review_headline) VALUES (?,?,?,?,?,?)"
        prepared = self.session.prepare(query)
        for index, item in df_reviews.iterrows():
            self.session.execute(prepared, (
                str(item.review_id), str(item.product_id), str(item.customer_id), int(float(item.star_rating)), datetime.strptime(item.review_date, '%Y-%m-%d'), item.review_headline))

    def insert_custom_reviews(self):
        col_list_customers = ['customer_id', 'review_id', 'product_id', 'star_rating', 'verified_purchase',
                              'review_date',
                              'review_headline']
        df_customers = pd.read_csv("amazon_reviews.tsv", delimiter='\t',
                                   usecols=col_list_customers,
                                   nrows=620000)  # the full number of rows is 3105371, we take 20%, so it will be around 620000
        df_customers['review_date'].fillna('1000-01-01', inplace=True)
        df_customers['star_rating'].fillna(0, inplace=True)
        df_customers.fillna('-', inplace=True)
        query = "INSERT INTO customer_reviews(customer_id, review_id,product_id, star_rating, verified_purchase, review_date,review_headline) VALUES (?,?,?,?,?,?,?)"
        prepared = self.session.prepare(query)
        for index, item in df_customers.iterrows():
            self.session.execute(prepared, (
                str(item.customer_id), str(item.review_id), str(item.product_id), int(float(item.star_rating)),
                str(item.verified_purchase),
                datetime.strptime(item.review_date, '%Y-%m-%d'), item.review_headline))


if __name__ == '__main__':
    host = 'localhost'
    port = 9042
    keyspace = 'my_keyspace'
    create_new_csv('amazon_reviews_us_Books_v1_02.tsv.gz')
    client = CassandraClient(host, port, keyspace)
    client.connect()
    client.insert_custom_reviews()
    client.insert_reviews()
    client.close()
