import redis
from airflow.hooks.base_hook import BaseHook
from typing import Any

class RedisHook(BaseHook):
    def __init__(self, conn_id: str, *args, **kwargs):
        super.__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.client = self._get_client()
    
    def _get_client(self):
        connection = self.get_connection(self.conn_id)
        return redis.Redis(
            host=connection.host,
            port=connection.port or 6379,
            password=connection.password,
            db=0
        )
    
    def set(self, key: str, value: Any):
        return self.client.set(key, value)
    
    def get(self, key: str) -> Any:
        return self.client.get(key)