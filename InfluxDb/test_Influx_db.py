import unittest
from datetime import datetime

from influxdb_client import InfluxDBClient, Point
from dotenv import dotenv_values
from influxdb_client.client.query_api import QueryOptions
from influxdb_client.client.write_api import SYNCHRONOUS


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        config = dotenv_values('.env')
        self.client = InfluxDBClient(url=config['INFUXDB_URL'], token=config['TOKEN'], org=config['ORG'])
        self.bucket = config['BUCKET']

    def test_connection(self):
        client = self.client

        query_api = client.query_api()

        buckets_api = client.buckets_api()

        bucket = buckets_api.find_bucket_by_name(self.bucket)

        expected_name = 'Ämpäri'

        self.assertEqual(expected_name, bucket.name)

    def test_write(self):
        client = self.client

        query_api = client.query_api()
        write_api = client.write_api(write_options=SYNCHRONOUS)

        p = Point('my_measurement').tag('location', 'Prague').field('temperature', 9.9).time(datetime.utcnow())

        write_api.write(bucket=self.bucket, record=p)

    def test_read(self):
        client = self.client

        query_api = client.query_api()

        # loads a shitton of all sorts of metadata
        # tables = query_api.query(f'from(bucket: "{self.bucket}") |> range(start: -10m)')
        #
        # for table in tables:
        #     print(table)
        #     for row in table.records:
        #         print(row.values)

        q = '''
            from(bucket: stringParam)
              |> range(start: -1h, stop: now())
              |> filter(fn: (r) => r["_measurement"] == "my_measurement")
              |> filter(fn: (r) => r["_field"] == "temperature")
              |> filter(fn: (r) => r["location"] == "Prague")
              |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
              |> yield(name: "last")
        '''
        p = {
            "stringParam": self.bucket,
        }

        query_api = client.query_api(query_options=QueryOptions(profilers=["query", "operator"]))
        csv_result = query_api.query(query=q, params=p)

        for record in csv_result[0].records:
            print(record)


if __name__ == '__main__':
    unittest.main()
