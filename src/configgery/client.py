from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple, Optional, Any, Dict, Set
from uuid import UUID

from urllib3 import PoolManager


class ConfigurationMetadata(NamedTuple):
    configuration_id: UUID
    path: str
    md5: str
    version: int
    alias: Optional[str]


class DeviceGroupMetadata(NamedTuple):
    device_group_id: UUID
    device_group_version: int
    configurations_metadata: Set[ConfigurationMetadata]
    last_loaded: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            'device_group_id': str(self.device_group_id),
            'device_group_version': self.device_group_version,
            'configurations_metadata': [
                {
                    'configuration_id': str(config.configuration_id),
                    'path': config.path,
                    'md5': config.md5,
                    'version': config.version,
                    'alias': config.alias,
                }
                for config in self.configurations_metadata
            ],
            'last_loaded': self.last_loaded.isoformat(),
            'version': Client.CONFIG_FILE_VERSION,
        }

    @classmethod
    def from_dict(cls, data) -> DeviceGroupMetadata:
        return DeviceGroupMetadata(
            device_group_id=UUID(data['device_group_id']),
            device_group_version=data['device_group_version'],
            configurations_metadata=set([
                ConfigurationMetadata(
                    configuration_id=UUID(config['configuration_id']),
                    path=config['path'],
                    md5=config['md5'],
                    version=config['version'],
                    alias=config.get('alias'),
                )
                for config in data['configurations_metadata']
            ]),
            last_loaded=(datetime.fromisoformat(data['last_loaded']) if 'last_loaded' in data
                         else datetime.now(tz=timezone.utc))
        )


class Client:
    BASE_URL = 'https://device.api.configgery.com/'
    CONFIG_FILE_VERSION = 1

    def __init__(self, configurations_directory: Path, certificate: Path, private_key: Path):
        self._pool = PoolManager(cert_file=certificate, key_file=private_key)
        self.device_group_metadata: Optional[DeviceGroupMetadata] = None
        self.configurations_directory = Path(configurations_directory)

        self.configurations_directory = configurations_directory.joinpath('configurations')
        self.configurations_directory.mkdir(exist_ok=True)
        self.configurations_metadata_file = configurations_directory.joinpath('configurations.json')

        if self.configurations_metadata_file.exists():
            logging.info('Loading metadata file')
            self.load_metadata_file()
        else:
            logging.info('No metadata file found')
            self.save_metadata_file()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._pool.clear()

    def load_metadata_file(self):
        with self.configurations_metadata_file.open('r') as fp:
            data = json.load(fp)

        if data['version'] != Client.CONFIG_FILE_VERSION:
            logging.warning(f'Invalid file version {data["version"]}')
            self.device_group_metadata = None
        elif 'device_group_id' in data:
            self.device_group_metadata = DeviceGroupMetadata.from_dict(data)
        else:
            self.device_group_metadata = None

    def save_metadata_file(self):
        if self.device_group_metadata is None:
            logging.info('Saving empty metadata file')
            data = {
                'version': Client.CONFIG_FILE_VERSION,
            }
        else:
            logging.info('Saving metadata file')
            data = self.device_group_metadata.to_dict()
        self.configurations_metadata_file.write_text(json.dumps(data, indent=2))

    def check_current_configurations(self):
        pass

    def load_device_group_metadata(self):
        logging.info('Loading device group metadata')
        r = self._pool.request('GET', Client.BASE_URL + 'v1/current_configurations')
        data = json.loads(r.data.decode('utf-8'))
        self.device_group_metadata = DeviceGroupMetadata.from_dict(data)
        self.save_metadata_file()

    def download_configurations(self):
        pass
