import json
from datetime import datetime, timezone
from typing import NamedTuple, List, Optional
from uuid import UUID

from urllib3 import PoolManager


class Configuration(NamedTuple):
    configuration_id: UUID
    path: str
    md5: str
    version: int
    alias: Optional[str]


class CurrentConfigurations(NamedTuple):
    device_group_id: UUID
    device_group_version: int
    configurations: List[Configuration]
    last_loaded: datetime


class Client:
    BASE_URL = 'https://device.api.configgery.com/'

    def __init__(self, certificate_path: str, private_key_path: str):
        self._pool = PoolManager(
            cert_file=certificate_path,
            key_file=private_key_path,
        )
        self.current_configurations: Optional[CurrentConfigurations] = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._pool.clear()

    def load_current_configurations(self):
        r = self._pool.request('GET', Client.BASE_URL + 'v1/current_configurations')
        data = json.loads(r.data.decode('utf-8'))
        self.current_configurations = CurrentConfigurations(
            device_group_id=UUID(data['device_group_id']),
            device_group_version=data['device_group_version'],
            configurations=[
                Configuration(
                    configuration_id=UUID(config['configuration_id']),
                    path=config['path'],
                    md5=config['md5'],
                    version=config['version'],
                    alias=config.get('alias'),
                )
                for config in data['configurations']
            ],
            last_loaded=datetime.now(tz=timezone.utc)
        )
