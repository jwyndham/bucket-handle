import functools
from typing import Callable

from src.bucket_manager import s3BucketManager
from src.caching.manager import CallDetails


def cache_data(reader: Callable, writer: Callable):
    def decorator_cache_data(download_func):
        @functools.wraps(download_func)
        def wrapper_cache_data(obj: s3BucketManager, *args, **kwargs):
            if obj.cache_manager.dummy:
                value = CallDetails(
                    'null',
                    'dummy_to_trigger_call',
                    False,
                    True
                )
            else:
                args_repr = [repr(a) for a in args]
                kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
                call_signature = ", ".join(args_repr + kwargs_repr)

                call_details = CallDetails.from_call_str(
                    call_signature,
                    obj.cache_manager
                )

                if call_details.execute_call:
                    value = download_func(obj, *args, **kwargs)
                    writer(value, call_details.filepath)
                    obj.cache_manager.new_call_record(call_details)
                else:
                    value = reader(call_details.filepath)
            return value
        return wrapper_cache_data
    return decorator_cache_data