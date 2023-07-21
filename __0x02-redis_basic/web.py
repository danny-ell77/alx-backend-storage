import redis
import requests
from functools import wraps

redis__ = redis.Redis()


def cache_with_expiration(expiration: int) -> callable:
    def invoker(method: callable) -> callable:
        @wraps(method)
        def wrapper(url: str) -> str:
            if result := redis__.get(f"result:{url}"):
                redis__.incrby(f"count:{url}", 1)
                return result.decode("utf-8")
            result = method(url)
            redis__.incrby(f"count:{url}", 1)
            redis__.setex(f"result:{url}", result, expiration)
            return result

        return wrapper

    return invoker


@cache_with_expiration(10)
def get_page(url: str) -> str:
    return requests.get(url).text
