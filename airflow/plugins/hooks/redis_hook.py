import redis
from airflow.hooks.base_hook import BaseHook
import pickle
import json
from typing import Any, Optional


class RedisHook(BaseHook):
    def __init__(self, conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.client = self._get_client()
    
    def _get_client(self):
        connection = self.get_connection(self.conn_id)
        return redis.Redis(
            host=connection.host,
            port=connection.port or 6379,
            password=connection.password,
            db=0,
            decode_responses=True
        )
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        set_value = json.dumps(value)
        try:
            self.client.set(key, set_value, ex=ttl)
            self.log.info(f"Successfully set key {key} to redis")
        except Exception as e:
            self.log.error(f"Error set value {value}")
        finally:
            return value

    def get(self, key: str) -> Any:
        result = self.client.get(key)
        return json.loads(result) if result else None
    
    def get_keys(self, pattern: str):
        return self.client.keys(pattern)

    def delete(self, key: str):
        try:
            result = self.client.detele(key)
            if result:
                self.log.info(f"Successfully delete key {key}")
            else:
                self.log.info(f"Key {key} not found")
        except Exception as e:
            self.log.error(f"Failed delete key {key}: {e}")
            raise