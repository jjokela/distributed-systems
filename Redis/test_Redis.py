import unittest

import redis


class TestRedis(unittest.TestCase):

    # @unittest.skip('Requires a local Redis container being up and running')
    def test_something(self):

        client = redis.Redis(host='127.0.0.1', port=6379)

        client.set('testkey', 'Hellou from paitton!')
        val = client.get('testkey')

        client.zadd('vehicles', {'car': 0})
        client.zadd('vehicles', {'bike': 0})
        vehicles = client.zrange('vehicles', 0, -1, withscores=True)

        print(vehicles)

        self.assertEqual(b'Hellou from paitton!', val)
        self.assertEqual([(b'bike', 0.0), (b'car', 0.0)], vehicles)

    # def test_redis_flushall(self):
    #     client = redis.Redis(host='127.0.0.1', port=6379)
    #     client.flushall()

    def test_redis_get_all_keys(self):
        client = redis.Redis(host='127.0.0.1', port=6379)
        all_keys = client.keys('*')

        print(all_keys)

    def test_ttl_key(self):
        client = redis.Redis(host='127.0.0.1', port=6379)
        key = '127.0.0.1'

        ttl = client.ttl(key)
        print(ttl)

    def test_redis_rate_limit(self):

        client = redis.Redis(host='127.0.0.1', port=6379)

        ip_address = '127.0.0.1'

        current = client.llen(ip_address)
        print('current', current)

        # over the limit, return 429 to the client
        if current > 10:
            raise Exception('Too many requests')

        if not client.exists(ip_address):
            pipeline = client.pipeline()
            pipeline.rpush(ip_address, ip_address)
            pipeline.expire(ip_address, 60)
            pipeline.execute()
        else:
            client.rpushx(ip_address, ip_address)

        # now we can execute the api call

        '''
        Redis rate limiting that handles race conditions
        You can use redis lists instead of counters
        https://redis.io/commands/incr/
        
        FUNCTION LIMIT_API_CALL(ip)
        current = LLEN(ip)
        IF current > 10 THEN
            ERROR "too many requests per second"
        ELSE
            IF EXISTS(ip) == FALSE
                MULTI
                    RPUSH(ip,ip)
                    EXPIRE(ip,1)
                EXEC
            ELSE
                RPUSHX(ip,ip)
            END
            PERFORM_API_CALL()
        END
        '''
        pass
