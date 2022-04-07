import logging
import time
from threading import Thread

from google.cloud import pubsub_v1


def pull_message(project, subscription):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)

    def callback(message):
        logging.info(f"Received {message.data}.")
        event_type = message.attributes.get("event_type")
        logging.info(f" Event Type {event_type}..\n")
        logging.info(f" Messages {message.data}..\n")
        message.ack()

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback, await_callbacks_on_shutdown=True,
    )
    logging.info(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=60)
        except TimeoutError:
            streaming_pull_future.cancel()
            logging.info("Streaming pull future canceled.")


class MessagePuller(Thread):
    def __init__(self, project, subscription):
        Thread.__init__(self)
        self.project_id = project
        self.subscription_id = subscription
        self.start()

    def run(self):
        while True:
            try:
                pull_message(self.project_id, self.subscription_id)
                time.sleep(30)
            except Exception as ex:
                logging.info(f"Listening for messages on {self.subscription_id} threw an exception: {ex}.")
                time.sleep(30)
