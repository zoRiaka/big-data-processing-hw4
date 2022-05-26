from flask import Flask, request
from read_from_cassandra import CassandraClient
import json

# create the Flask app
app = Flask(__name__)


@app.route('/queries', methods=['GET'])
def query_example():
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    args = request.args
    if args.get("query") == '1':
        return json.dumps(client.query_1(args.get("product_id")), default=str, sort_keys=True, indent=4)
    elif args.get("query") == '2':
        return json.dumps(client.query_2(args.get("product_id"), int(args.get("star_rating"))), default=str)
    elif args.get("query") == '3':
        return json.dumps(client.query_3(args.get("customer_id")), default=str)
    elif args.get("query") == '4':
        return json.dumps(client.query_4(args.get("start"), args.get("end"), int(args.get("n"))), default=str)
    elif args.get("query") == '5':
        return json.dumps(client.query_5(args.get("start"), args.get("end"), int(args.get("n"))), default=str)
    elif args.get("query") == '6':
        return json.dumps(client.query_6(args.get("start"), args.get("end"), int(args.get("n"))), default=str)
    elif args.get("query") == '7':
        return json.dumps(client.query_7(args.get("start"), args.get("end"), int(args.get("n"))), default=str)
    else:
        exit(-2)


if __name__ == '__main__':
    host = 'localhost'
    port = 9042
    keyspace = 'my_keyspace'

    client = CassandraClient(host, port, keyspace)
    client.connect()
    app.run(port=5000)
