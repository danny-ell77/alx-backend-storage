import redis
from functools import wraps
from typing import Callable, Union
from uuid import uuid4


def count_calls(method: Callable) -> Callable:
    @wraps(method)
    def some_method(self, *args, **kwargs):
        if isinstance(self._redis, redis.Redis):
            self._redis.incrby(method.__qualname__, 1)
        return method(self, *args, **kwargs)

    return some_method


def call_history(method: Callable) -> Callable:
    @wraps(method)
    def some_method(self, *args, **kwargs):
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(f"{method.__qualname__}:inputs", str(args))
        output = method(self, *args, **kwargs)
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(f"{method.__qualname__}:outputs", str(output))
        return output

    return some_method


def replay(method: Callable):
    if method is None or not hasattr(method, "__self__"):
        return
    redis_store = getattr(method.__self__, "_redis", None)
    if not isinstance(redis_store, redis.Redis):
        return
    method_name = method.__qualname__
    num_calls = redis_store.get(method_name)
    args = redis_store.lrange(f"{method_name}:inputs", 0, -1)
    outputs = redis_store.lrange(f"{method_name}:outputs", 0, -1)
    print(f"{method_name} was called {num_calls}")
    for arg, output in zip(args, outputs):
        print(f"{method_name}(*{arg.decode('utf-8')}) -> {output.decode('utf-8')}")


class Cache:
    def __init__(self):
        self._redis = redis.Redis()
        self._redis.flushdb()

    @call_history
    @count_calls
    def store(self, data: Union[str, bytes, int, float]) -> str:
        key = str(uuid4())
        self._redis.set(key, data)
        return key

    def get(self, key: str, fn: Callable = None) -> Union[str, bytes, int, float]:
        value = self._redis.get(key)
        if fn is not None:
            return fn(value)
        else:
            return value

    def get_int(value: bytes) -> int:
        return int(value.decode("utf-8"))

    def get_str(value: bytes) -> str:
        return str(value.decode("utf-8"))
