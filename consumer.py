import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from tweepy import StreamListener, Stream, OAuthHandler


if __name__ == "__main__":
    consumer = KafkaConsumer('twitter', group_id='twitter-local', bootstrap_servers=['127.0.0.1:9092'], value_deserializer=lambda m:json.loads(m.decode('utf-8')))
    for message in consumer:
        # print("Id:", message.key)
        # print("Offset:", message.offset)
        # print("Keys:", message.value.keys())
        value = json.loads(message.value[list(message.value.keys())[0]])
        # print(value.keys())
        print("Value:", value.get('entities').get('hashtags'))
