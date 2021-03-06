import boto3
import json
from io import StringIO, BytesIO
import pandas as pd
from dataclasses import dataclass

from handle.caching.manager import CacheManager
from handle.caching.decorators import cache_data
from handle.read_write import (
    read_json,
    write_json,
)


@dataclass
class BucketManager:
    bucket: str
    aws_id: str
    aws_key: str
    cache_manager: CacheManager = None

    def __post_init__(self):
        if not self.cache_manager:
            self.cache_manager = CacheManager('dummy_cache', 1, dummy=True)

    @property
    def resource(self):
        return boto3.resource(
            's3',
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_key
        )

    @staticmethod
    def path(folders, fn):
        folders_str = '/'.join(folders)
        return f'{folders_str}/{fn}'

    def df_to_s3_csv(
            self,
            df: pd.DataFrame,
            folders,
            fn,
            **kwargs
    ):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, **kwargs)
        self.resource.Object(
            self.bucket,
            self.path(folders, fn),
        ).put(Body=csv_buffer.getvalue())

    def dict_to_s3_json(
            self,
            json_data: dict,
            folders,
            fn,
            indent=2
    ):
        self.resource.Object(
            self.bucket,
            self.path(folders, fn),
        ).put(Body=bytes(
            json.dumps(
                json_data,
                indent=indent
            ).encode('UTF-8')))

    @cache_data(read_json, write_json)
    def s3_json_to_dict(
            self,
            folders,
            fn
    ) -> dict:
        file_object = self.resource.Object(
            self.bucket,
            self.path(folders, fn),
        )
        file = file_object.get()['Body'].read().decode('utf-8')
        json_data = json.loads(file)
        return json_data

    def df_to_s3_ftr(
            self,
            df: pd.DataFrame,
            folders,
            fn,
            **kwargs
    ):
        buffer = BytesIO()
        df.to_feather(buffer, **kwargs)
        self.resource.Object(
            self.bucket,
            self.path(folders, fn),
        ).put(Body=buffer.getvalue())

    @cache_data(pd.read_feather, pd.DataFrame.to_feather)
    def s3_ftr_to_df(
            self,
            folders,
            fn
    ) -> pd.DataFrame:
        file_object = self.resource.Object(
            self.bucket,
            self.path(folders, fn)
        )
        file = BytesIO(file_object.get()['Body'].read())
        return pd.read_feather(file)

    @cache_data(pd.read_feather, pd.DataFrame.to_feather)
    def s3_csv_to_df(
            self,
            folders,
            fn,
    ) -> pd.DataFrame:
        file_object = self.resource.Object(
            self.bucket,
            self.path(folders, fn)
        )
        file = BytesIO(file_object.get()['Body'].read())
        return pd.read_csv(file)
