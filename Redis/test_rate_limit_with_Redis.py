import unittest

import redis


class RateLimiter:
    def __init__(self, request_limit, timedelta):
        self.limit = request_limit
        self.timedelta = timedelta
        self.redis = redis.Redis(host='127.0.0.1', port=6379)

    def check_rate_limit(self, user):
        """
        Redis rate limiting that handles race conditions
        You can use redis lists instead of counters
        https://redis.io/commands/incr/
        :param user: user
        :return: True if request is allowed, False if request is not allowed
        """

        client = self.redis

        current = client.llen(user)

        if current >= self.limit:
            return False

        if not client.exists(user):
            pipeline = client.pipeline()
            pipeline.rpush(user, user)
            pipeline.expire(user, self.timedelta)
            pipeline.execute()
        else:
            client.rpushx(user, user)

        return True


class MyTestCase(unittest.TestCase):
    def test_rate_limit(self):
        rate_limiter = RateLimiter(request_limit=5, timedelta=2)
        user = 'test_user'
        for i in range(5):
            result = rate_limiter.check_rate_limit(user=user)
            print(result)
            self.assertEqual(True, result)

    def test_rate_limit_too_many_requests(self):
        rate_limiter = RateLimiter(request_limit=5, timedelta=2)
        user = 'test_user'
        for i in range(5):
            result = rate_limiter.check_rate_limit(user=user)
            print(result)
            self.assertEqual(True, result)

        self.assertEqual(False, rate_limiter.check_rate_limit(user=user))


if __name__ == '__main__':
    unittest.main()
