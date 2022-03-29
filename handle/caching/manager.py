from __future__ import annotations


import json
import os
from dataclasses import dataclass, field
from time import time, time_ns


def unique_str():
    return str(time_ns())


@dataclass(frozen=True)
class CacheAction:
    """ Statement of whether a new cache file should be created
    and where it should go for a given call signature
    """
    get_signature: str
    refresh: bool
    filepath: str


@dataclass
class CacheManager:
    """ Class for defining params of cache, managing log
    and identifying whether files need creating/refreshing
    """
    directory: str
    expiry_seconds: int
    cache_metadata: dict = field(default_factory={})
    dummy: bool = False

    @property
    def log_path(self) -> str:
        return os.path.join(self.directory, 'cache_log.json')

    def __post_init__(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        if not os.path.exists(self.log_path):
            self.commit_log_updates({
                'metadata': self.cache_metadata,
                'filepaths': {}
            })

    def _new_fp(self):
        return os.path.join(self.directory, unique_str())

    def retrieve_log(self):
        with open(self.log_path, 'r') as f:
            file = json.load(f)
        return file

    def commit_log_updates(self, obj: dict):
        with open(self.log_path, 'w') as f:
            json.dump(obj, f, indent=2)

    def cached_fp(self, call_str) -> str:
        return self.retrieve_log()['filepaths'][call_str]

    def file_exists(self, call_str):
        return call_str in self.retrieve_log()['filepaths'].keys()

    def refresh_needed(self, fp):
        return time() - os.path.getmtime(fp) > self.expiry_seconds

    def action(self, call_str) -> CacheAction:
        if self.file_exists(call_str):
            fp = self.cached_fp(call_str)
            refresh_needed = self.refresh_needed(fp)
        else:
            fp = self._new_fp()
            refresh_needed = False
        return CacheAction(call_str, refresh_needed, fp)

    def log_new_file(self, action: CacheAction):
        log = self.retrieve_log()
        log['filepaths'][action.get_signature] = action.filepath
