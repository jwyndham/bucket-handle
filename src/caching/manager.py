from __future__ import annotations


import json
import os
from dataclasses import dataclass
from time import time
from typing import Callable


@dataclass(frozen=True)
class CallDetails:
    call_signature: str
    filepath: str
    life: int
    exists: bool = False

    def expired(self, life: int):
        return time() - os.path.getmtime(self.filepath) > life

    def execute_call(self) -> bool:
        if not self.exists:
            return True
        if self.expired(self.life):
            return True
        else:
            return False

    @classmethod
    def from_call_str(
            cls,
            call_str: str,
            cache_manager: CacheManager
    ):
        log = cache_manager.retrieve_log()
        if call_str in log.keys():
            fp = log[call_str]
            exists = os.path.exists(fp)
        else:
            fp = os.path.join(
                cache_manager.directory,
                cache_manager.next_cache_file_name()
            )
            exists = False
        return cls(call_str, fp, cache_manager.life, exists)


@dataclass
class CacheManager:
    directory: str
    life: int
    dummy: bool = False

    @property
    def log_path(self) -> str:
        return os.path.join(self.directory, 'log.json')

    def __post_init__(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        if not os.path.exists(self.log_path):
            self.update_log({'next_call_ref_n': 0})

    def retrieve_log(self):
        with open(self.log_path, 'r') as f:
            file = json.load(f)
        return file

    def update_log(self, obj: dict):
        with open(self.log_path, 'w') as f:
            json.dump(obj, f, indent=2)

    def next_cache_file_name(self):
        log = self.retrieve_log()
        name = str(log['next_call_ref_n']).zfill(10)
        log['next_call_ref_n'] += 1
        self.update_log(log)
        return name

    def call_details(self, call_str: str) -> CallDetails:
        log = self.retrieve_log()
        if call_str in log.keys():
            fp = log[call_str]
            exists = os.path.exists(fp)
        else:
            fp = os.path.join(self.directory, self.next_cache_file_name())
            exists = False
        return CallDetails(call_str, fp, self.life, exists)

    def new_call_record(self, call_details: CallDetails):
        if not self.dummy:
            log = self.retrieve_log()
            log[call_details.call_signature] = call_details.filepath
            self.update_log(log)
