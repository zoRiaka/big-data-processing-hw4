import time


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

    def query_1(self, product_id):
        print('QUERY1')
        query = "select review_id, review_headline from reviews where product_id = '%s';" % product_id
        rows = self.session.execute(query)
        return dict(rows)

    def query_2(self, product_id, star_rating):
        print('QUERY2')
        query = "select review_id, review_headline from reviews where product_id = '%s' AND star_rating = %i;" % (
            product_id, star_rating)
        rows = self.session.execute(query)
        return dict(rows)

    def query_3(self, customer_id):
        print('QUERY3')
        query = "select review_id, review_headline from customer_reviews where customer_id='%s';" % customer_id
        rows = self.session.execute(query)
        return dict(rows)

    def query_4(self, start_date, end_date, n):
        print('QUERY4')
        query = "select product_id, COUNT(*) from reviews where review_date > '%s' and review_date < '%s' group by product_id ALLOW FILTERING;" % (
            start_date, end_date)
        rows = self.session.execute(query)
        rows = sorted(rows, key=lambda item: item[1], reverse=True)
        return dict(rows[0:n])

    def query_5(self, start_date, end_date, n):
        print('QUERY5')
        query = "select customer_id, COUNT(*) from customer_reviews where review_date > '%s' and review_date < '%s' and verified_purchase='Y' group by customer_id ALLOW FILTERING;" % (
            start_date, end_date)
        rows = self.session.execute(query)
        rows = sorted(rows, key=lambda item: item[1], reverse=True)
        return dict(rows[0:n])

    def query_6(self, start_date, end_date, n):
        print('QUERY6')
        query = "select customer_id, COUNT(*) from customer_reviews where review_date > '%s' and review_date < '%s' and star_rating <=2  group by customer_id ALLOW FILTERING;" % (
            start_date, end_date)
        rows = self.session.execute(query)
        rows = sorted(rows, key=lambda item: item[1], reverse=True)
        return dict(rows[0:n])

    def query_7(self, start_date, end_date, n):
        print('QUERY7')
        query = "select customer_id, COUNT(*) from customer_reviews where review_date > '%s' and review_date < '%s' and star_rating >=4  group by customer_id ALLOW FILTERING;" % (
            start_date, end_date)
        rows = self.session.execute(query)
        rows = sorted(rows, key=lambda item: item[1], reverse=True)
        return dict(rows[0:n])


if __name__ == '__main__':
    host = 'localhost'
    port = 9042
    keyspace = 'my_keyspace'

    client = CassandraClient(host, port, keyspace)
    client.connect()
    # client.query_1('0140291784')
    # client.query_2('0140291784', 3)
    # client.query_3('52837740')
    # client.query_4('2001-01-01', '2022-01-01', 10)
    # client.query_5('2001-01-01', '2022-01-01', 10)
    # client.query_6('2001-01-01', '2022-01-01', 10)
    # client.query_7('2001-01-01', '2022-01-01', 10)
    client.close()
