from __future__ import annotations

import json
import logging
from binascii import hexlify
from datetime import datetime, timezone
from enum import Enum, auto
from hashlib import md5
from itertools import chain
from pathlib import Path
from typing import NamedTuple, Optional, Any, Dict, Set, Generator
from uuid import UUID

from urllib3 import PoolManager, HTTPResponse


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


def file_md5(path: Path) -> str:
    # noinspection PyBroadException
    try:
        with path.open('rb') as fp:
            return hexlify(md5(fp.read()).digest()).decode()
    except (FileNotFoundError, PermissionError):
        return ''
    except BaseException as e:
        logging.warning(f'Unexpected exception when reading md5: {str(e)}')
        return ''


def remove_dir_if_empty(root: Path):
    for d in root.iterdir():
        if d.is_dir():
            remove_dir_if_empty(d)
        try:
            d.rmdir()
        except OSError:
            # Directory not empty
            pass


class State(Enum):
    Outdated = auto()
    MetadataDownloaded = auto()
    Valid = auto()

    Invalid_FailedToLoadMetadata = auto()
    Invalid_FailedToDownload = auto()


class Client:
    BASE_URL = 'https://device.api.configgery.com/'
    CONFIG_FILE_VERSION = 1

    def __init__(self, configurations_directory: Path, certificate: Path, private_key: Path):
        self.state: State = State.Outdated
        self._pool = PoolManager(cert_file=certificate, key_file=private_key)
        self.device_group_metadata: Optional[DeviceGroupMetadata] = None
        self.configurations_directory = Path(configurations_directory)

        self.configurations_directory = configurations_directory.joinpath('configurations')
        self.configurations_directory.mkdir(exist_ok=True)
        self.configurations_metadata_file = configurations_directory.joinpath('configurations.json')

        if self.configurations_metadata_file.exists():
            logging.info('Loading metadata file')
            self._load_metadata_file()
        else:
            logging.info('No metadata file found')
            self._save_metadata_file()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._pool.clear()

    def _load_metadata_file(self):
        with self.configurations_metadata_file.open('r') as fp:
            data = json.load(fp)

        if data['version'] != Client.CONFIG_FILE_VERSION:
            logging.warning(f'Invalid file version {data["version"]}')
            self.device_group_metadata = None
        elif 'device_group_id' in data:
            self.device_group_metadata = DeviceGroupMetadata.from_dict(data)
        else:
            self.device_group_metadata = None

    def _save_metadata_file(self):
        if self.device_group_metadata is None:
            logging.info('Saving empty metadata file')
            data = {
                'version': Client.CONFIG_FILE_VERSION,
            }
        else:
            logging.info('Saving metadata file')
            data = self.device_group_metadata.to_dict()
        self.configurations_metadata_file.write_text(json.dumps(data, indent=2))

    def _remove_old_configurations(self):
        logging.info('Removing old configurations')
        if self.device_group_metadata is None:
            logging.error('Unable to remove old configurations without device group metadata')
            return

        valid_paths = {config.path for config in self.device_group_metadata.configurations_metadata}
        if self.device_group_metadata is not None:
            for file in chain(self.configurations_directory.glob('**/*'), self.configurations_directory.glob('*')):
                try:
                    rel_path = file.relative_to(self.configurations_directory)
                    if file.is_file() and str(rel_path) not in valid_paths:
                        try:
                            logging.debug(f'Deleting file {file}')
                            file.unlink()
                        except FileNotFoundError:
                            logging.warning(f'Could not delete file {file}')
                            # Do nothing
                            pass
                except ValueError:
                    logging.error(f'Could not understand path for file {file}')

            remove_dir_if_empty(self.configurations_directory)

    def outdated_configurations(self) -> Generator[ConfigurationMetadata, None, None]:
        for config in self.device_group_metadata.configurations_metadata:
            if config.md5 != file_md5(self.configurations_directory.joinpath(config.path)):
                yield config

    def load_device_group_metadata(self):
        logging.info('Loading device group metadata')
        r: HTTPResponse = self._pool.request('GET', Client.BASE_URL + 'v1/current_configurations')
        if r.status == 200:
            data = json.loads(r.data.decode('utf-8'))
            self.device_group_metadata = DeviceGroupMetadata.from_dict(data)
            self._save_metadata_file()
            self.state = State.MetadataDownloaded
        else:
            logging.error(f'Failed to get current device group: {r.status}: {r.data.decode("utf-8")}')
            self.state = State.Invalid_FailedToLoadMetadata
            self.device_group_metadata = None

    def download_configurations(self) -> bool:
        if self.device_group_metadata is None:
            self.load_device_group_metadata()

        if self.device_group_metadata is None:
            return False

        self._remove_old_configurations()

        all_ok = True
        for config in self.outdated_configurations():
            path = self.configurations_directory.joinpath(config.path)
            path.parent.mkdir(exist_ok=True)
            with path.open('wb') as fp:
                r = self._pool.request('GET', Client.BASE_URL + 'v1/configuration', fields={
                    'configuration_id': config.configuration_id,
                    'version': config.version
                })
                if r.status == 200:
                    fp.write(r.data)
                else:
                    logging.error((f'Failed to get configuration {config.configuration_id} version {config.version}. '
                                   f'Received response {r.status}: {r.data.decode("utf-8")}'))
                    self.state = State.Invalid_FailedToDownload
                    all_ok = False

        if all_ok:
            self.state = State.Valid
            return True
        else:
            return False
