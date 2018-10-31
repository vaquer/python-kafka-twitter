import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tweepy import StreamListener, Stream, OAuthHandler


class BirdListener(StreamListener):
    def __init__(self, *args, **kwargs):
        super(BirdListener, self).__init__(*args, **kwargs)

        self.kafka_producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'), bootstrap_servers=['127.0.0.1:9092'])

    def on_data(self, data):
        print(data)
        data_json = json.loads(data, encoding='utf-8')
        future = self.kafka_producer.send('twitter', { data_json.get('id', 'None'): data})
        try:
            record_metadata = future.get(timeout=5)
        except KafkaError as kafka_error:
            print(kafka_error)

        print("Topic: ", record_metadata.topic)
        print("partition: ", record_metadata.partition)
        print("offset: ",record_metadata.offset)

        return True


if __name__ == '__main__':
    consumer_key = ''
    consumer_secret = ''

    access_token = ''
    access_token_secret = ''

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    my_listener = BirdListener()

    my_stream = Stream(auth=auth, listener=my_listener)
    my_stream.filter(track=['coca cola'])
