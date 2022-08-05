"""
This script subscribes to the appropriate channels for the UrbanRace game
and rebroadcasts them to DynamoDB.

Assumes that AWS credentials are set up.

When switching to use the AWS IoT API, can rebroadcast using Rules Engine instead.
"""
import argparse
import json
import typing as T

import boto3
from botocore.client import DynamoDB
from paho.mqtt import client as mqtt_client

CLIENT_ID = "mqtt-listener"

# report count of invalid messages periodically
VALID_MESSAGES = 0
INVALID_MESSAGES = 0

BOTO_CLIENT: boto3.Dyn = boto3.client("dynamodb")


def make_db_item_from_dict(data: T.Dict[str, T.Union[str, float, bytes]]) -> T.Dict:
    """
    Given an input key-value dictionary, recursively determine types and create a dictionary
    suitable for ingest into a DynamoDB instance.

    TODO: implement for sets
    """
    dynamodict = dict()
    for key, value in data.items():
        if isinstance(value, dict):
            # recurse
            child = make_db_item_from_dict(value)
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

        try:
            payload = message.payload
            data: T.Dict = json.loads(payload)

            # do some basic sanity checking to make sure the message is valid
            if 'client_id' not in data:
                INVALID_MESSAGES += 1
                raise ValueError("client_id not in payload")
            if 'timestamp' not in data:
                float(data['timestamp'])
                raise ValueError("client_id not in payload")

            db_item = make_db_item_from_dict(data)
            BOTO_CLIENT.put_item(TableName=tablename, Item=db_item)
            VALID_MESSAGES += 1

        except BaseException as exc:
            INVALID_MESSAGES += 1
            print(f"Could not ingest message: {repr(exc)}")

    for topic in topics:
        client.subscribe(topic)
    client.on_message = ingest_message


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", default="urban-race-demo")
    parser.add_argument("--topics", nargs="+", help="list of topics to listen to")
    parser.add_argument("--broker-uri", default="3.17.24.212:1883")
    parser.add_argument("--username", required=True, help="MQTT username")
    parser.add_argument("--password", required=True, help="MQTT password")
    args = parser.parse_args()

    boto_client: DynamoDB = boto3.client("dynamodb")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Listener is connected to MQTT broker!")
        else:
            print(f"Failed to connect to MQTT broker, return code {rc}")

    # Listen for MQTT messages here
    client = mqtt_client.Client(client_id=CLIENT_ID)
    client.username_pw_set(args.username, args.password)
    client.on_connect = on_connect

    subscribe_topics(client, args.topics)

    client.loop_forever()


if __name__ == "__main__":
    main()

