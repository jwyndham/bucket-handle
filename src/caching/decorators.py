import functools
from typing import Callable

from src.bucket_manager import BucketManager


def cache_data(reader: Callable, writer: Callable):
    def decorator_cache_data(get_method):
        @functools.wraps(get_method)
        def wrapper_cache_data(obj: BucketManager, *args, **kwargs):
            cache = obj.cache_manager
            if cache.dummy:
                value = get_method(obj, *args, **kwargs)
            else:
                args_repr = [repr(a) for a in args]
                kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
                get_signature = ", ".join(args_repr + kwargs_repr)

                action = cache.action(get_signature)
                if action.refresh:
                    value = get_method(obj, *args, **kwargs)
                    writer(value, action.filepath)
                    cache.log_new_file(action)
                else:
                    value = reader(action.filepath)
            return value
        return wrapper_cache_data
    return decorator_cache_data
