"""
This script subscribes to the appropriate channels for the UrbanRace game
and rebroadcasts them to DynamoDB.

Assumes that AWS credentials are set up.

When switching to use the AWS IoT API, can rebroadcast using Rules Engine instead.
"""
import argparse
import json
import time
import typing as T
from threading import Thread

import boto3
from paho.mqtt import client as mqtt_client

CLIENT_ID = "mqtt-listener"
BOTO_CLIENT = boto3.client("dynamodb")

class MessageCounter:
    """
    Shared object which updates message counts
    """

    def __init__(self) -> None:
        self.valid_messages = 0
        self.invalid_messages = 0

    def add_valid_messages(self, count: int = 1):
        self.valid_messages += count

    def add_invalid_messages(self, count: int = 1):
        self.invalid_messages += count


MESSAGE_COUNTER = MessageCounter()


def make_db_item_from_dict(data: T.Dict[str, T.Union[str, float, bytes]]) -> T.Dict:
    """
    Given an input key-value dictionary, recursively determine types and create a dictionary
    suitable for ingest into a DynamoDB instance.

    TODO: implement for sets
    """
    dynamodict = dict()
    for key, value in data.items():
        if isinstance(value, dict):
            # recurse and flatten
            nested = make_db_item_from_dict(value)
            for child_key, child_value in nested.items():
                dynamodict[f"{key}.{child_key}"] = child_value
            continue
        elif isinstance(value, str):
            child = {"S": str(value)}
        elif isinstance(value, float) or isinstance(value, int):
            child = {"N": str(value)}
        elif isinstance(value, bytes):
            child = {"B": value}
        else:
            raise ValueError(f"I don't know how to handle {value}")

        dynamodict[key] = child

    return dynamodict


def subscribe_topics(client: mqtt_client.Client, tablename: str, topics: T.List[str]) -> None:
    """
    Subscribe to all topics
    """

    def ingest_message(
        _: T.Any,
        userdata: T.Any,
        message: mqtt_client.MQTTMessage
    ) -> None:
        """
        The ingest step should break down the message key / value pairs and feed them into
        DynamoDB.

        The entire function should be wrapped in a try-except
        """

        global MESSAGE_COUNTER

        try:
            payload = message.payload
            data: T.Dict = json.loads(payload)

            # do some basic sanity checking to make sure the message is valid
            if 'client_id' not in data:
                raise ValueError("client_id not in payload")
            if 'timestamp' not in data:
                float(data['timestamp'])
                raise ValueError("client_id not in payload")

            db_item = make_db_item_from_dict(data)
            BOTO_CLIENT.put_item(TableName=tablename, Item=db_item)
            MESSAGE_COUNTER.add_valid_messages()

        except BaseException as exc:
            MESSAGE_COUNTER.add_invalid_messages()
            print(f"Could not ingest message: {repr(exc)}")

    for topic in topics:
        client.subscribe(topic)
    client.on_message = ingest_message


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", default="urban-race-demo")
    parser.add_argument("--topics", nargs="+", help="list of topics to listen to")
    parser.add_argument("--broker-host", default="3.17.24.212")
    parser.add_argument("--broker-port", default=1883, type=int)
    parser.add_argument("--username", required=True, help="MQTT username")
    parser.add_argument("--password", required=True, help="MQTT password")
    args = parser.parse_args()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Listener is connected to MQTT broker!")
        else:
            print(f"Failed to connect to MQTT broker, return code {rc}")

    # Listen for MQTT messages here
    client = mqtt_client.Client(client_id=CLIENT_ID)
    client.username_pw_set(args.username, args.password)
    client.on_connect = on_connect
    client.connect(args.broker_host, args.broker_port)

    subscribe_topics(client, args.table_name, args.topics)

    # Update with messages on how many messages have been processed total
    def _print_message_counts(message_counter: MessageCounter, frequency: float = 6.0):
        """
        Periodically print out message counts
        """
        while True:
            print(f"Successfully uploaded {message_counter.valid_messages}. "
                  f"Failed to upload {message_counter.invalid_messages}.")
            time.sleep(frequency)

    update_thread = Thread(target=_print_message_counts, args=(MESSAGE_COUNTER, ))
    update_thread.daemon = True
    update_thread.start()

    client.loop_forever()


if __name__ == "__main__":
    main()

