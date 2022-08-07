import unittest
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv, dotenv_values


class MyTestCase(unittest.TestCase):
    def test_something(self):

        config = dotenv_values(".env")

        client = InfluxDBClient(url=config['INFUXDB_URL'], token=config['TOKEN'], org=config['ORG'])

        query_api = client.query_api()

        buckets_api = client.buckets_api()

        bucket = buckets_api.find_bucket_by_name(config['BUCKET'])

        expected_name = 'Ämpäri'

        self.assertEqual(expected_name, bucket.name)


if __name__ == '__main__':
    unittest.main()
