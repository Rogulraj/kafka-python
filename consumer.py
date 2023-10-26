from kafka import KafkaConsumer

import json

if __name__ == "__main__":
    consumer = KafkaConsumer("user_data", bootstrap_servers="localhost:9092",
                             auto_offset_reset="earliest", group_id="consumer-group-1")
    print("Consuming Started")
    for msg in consumer:
        print("user_data = {}".format(json.loads(msg.value)))
