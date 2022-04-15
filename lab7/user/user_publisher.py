import json
import logging

from google.cloud import pubsub_v1


# Code is based on the following examples from Google. Please check them for more information.
# https://github.com/googleapis/python-pubsub/blob/master/samples/snippets/publisher.py
# https://github.com/googleapis/python-pubsub/blob/master/samples/snippets/subscriber.py

def create_topic(project_id, topic_id):
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        topic = publisher.create_topic(request={"name": topic_path})
        logging.info("Created topic: {}".format(topic.name))
    except Exception as ex:
        logging.info(ex)  # instead, can check if there is a topic already, and only if not create a new one


def publish_message(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    future = publisher.publish(topic_path, message)
    try:
        future.result()  # see https://docs.python.org/3/library/concurrent.futures.html
    except Exception as ex:
        logging.info(ex)
    logging.info(f"Published messages to {topic_path}.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    create_topic("jads-de-2021", "order_req")  # make sure to change the project id - i.e., ada2022-341617
    data = {
        "product_type": "Phone",
        "quantity": 100000000000,
        "unit_price": 232.00
    }
    data = json.dumps(data).encode("utf-8")
    publish_message("jads-de-2021", "order_req", data)
