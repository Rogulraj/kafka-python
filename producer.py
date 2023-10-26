from kafka import KafkaProducer
from json import dumps
import time

from data import get_registered_user


def json_serializer(data):
    return dumps(data).encode("utf-8")


def get_partition(key, all, available):
    return 0


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)

wait = 5
if __name__ == "__main__":
    while True:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send("user_data", registered_user)
        time.sleep(wait)
