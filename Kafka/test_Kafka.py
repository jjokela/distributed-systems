import unittest

from kafka import KafkaConsumer, KafkaProducer


class MyTestCase(unittest.TestCase):
    def test_consume_messages(self):
        consumer = KafkaConsumer('first_kafka_topic',
                                 bootstrap_servers=['localhost:9092'],
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=False)

        for message in consumer:
            print(message)

        self.assertEqual(True, False)  # add assertion here

    def test_producre_messages(self):
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        result = producer.send('first_kafka_topic', b'hello world')

        print(result)


if __name__ == '__main__':
    unittest.main()
