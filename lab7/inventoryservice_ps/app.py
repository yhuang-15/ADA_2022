import logging
import os

from flask import Flask, request

from message_puller import MessagePuller
from pub_sub_util import create_subscription, create_topic
from resources.product import Product, Products

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
product = Product()
products = Products()
project_id = os.environ['project_id']
create_topic(project=project_id, topic="order_req")
create_subscription(project=project_id, topic="order_req", subscription="order_req_sub")
create_topic(project=project_id, topic="inventory_status")
create_topic(project=project_id, topic="order_status")
create_subscription(project=project_id, topic="order_status", subscription="order_status_sub")
MessagePuller(project=project_id, subscription="order_req_sub", product=product)
MessagePuller(project=project_id, subscription="order_status_sub", product=product)

app.run(host='0.0.0.0', port=5000, debug=True)
