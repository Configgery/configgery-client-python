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

log = logging.getLogger(__name__)


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
    last_checked: datetime

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
                for config in sorted(self.configurations_metadata, key=lambda x: x.path)
            ],
            'last_checked': self.last_checked.isoformat(),
            'version': Client.CONFIG_FILE_VERSION,
        }

    @classmethod
    def from_server(cls, data) -> DeviceGroupMetadata:
        return DeviceGroupMetadata(
            device_group_id=UUID(data['device_group_id']),
            device_group_version=data['device_group_version'],
            configurations_metadata={
                ConfigurationMetadata(
                    configuration_id=UUID(config['configuration_id']),
                    path=config['path'],
                    md5=config['md5'],
                    version=config['version'],
                    alias=config.get('alias'),
                )
                for config in data['configurations']
            },
            last_checked=datetime.now(tz=timezone.utc)
        )

    @classmethod
    def from_dict(cls, data) -> DeviceGroupMetadata:
        return DeviceGroupMetadata(
            device_group_id=UUID(data['device_group_id']),
            device_group_version=data['device_group_version'],
            configurations_metadata={
                ConfigurationMetadata(
                    configuration_id=UUID(config['configuration_id']),
                    path=config['path'],
                    md5=config['md5'],
                    version=config['version'],
                    alias=config.get('alias'),
                )
                for config in data['configurations_metadata']
            },
            last_checked=datetime.fromisoformat(data['last_checked'])
        )


def file_md5(path: Path) -> str:
    # noinspection PyBroadException
    try:
        with path.open('rb') as fp:
            return hexlify(md5(fp.read()).digest()).decode()
    except (FileNotFoundError, PermissionError):
        return ''
    except BaseException as e:
        log.exception(f'Unexpected exception when reading md5')
        return ''


def remove_subdirs_if_empty(root: Path):
    for d in root.iterdir():
        if d.is_dir():
            remove_subdirs_if_empty(d)
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


class DeviceState(Enum):
    ConfigurationsApplied = auto()
    Upvote = auto()
    Downvote = auto()


class Client:
    BASE_URL = 'https://device.api.configgery.com/'
    CONFIG_FILE_VERSION = 1

    def __init__(self, configurations_directory: Path, certificate: Path, private_key: Path):
        self._state: State = State.Outdated
        self._pool = PoolManager(cert_file=certificate, key_file=private_key)
        self._device_group_metadata: Optional[DeviceGroupMetadata] = None
        self._configurations_directory = Path(configurations_directory)

        self._configurations_directory = configurations_directory.joinpath('configurations')
        self._configurations_directory.mkdir(exist_ok=True)
        self._configurations_metadata_file = configurations_directory.joinpath('configurations.json')

        if self._configurations_metadata_file.exists():
            log.info('Loading cached configuration data')
            self._load_metadata_file()
        else:
            log.info('No cached configuration data found')

    def _load_metadata_file(self):
        try:
            with self._configurations_metadata_file.open('r') as fp:
                data = json.load(fp)
        except (FileNotFoundError, PermissionError, json.JSONDecodeError) as e:
            log.exception(f'Unable to read cached configuration data')
            self._device_group_metadata = None
        else:
            if data['version'] != Client.CONFIG_FILE_VERSION:
                log.warning(f'Invalid file version {data["version"]}')
                self._device_group_metadata = None
            elif 'device_group_id' in data:
                self._device_group_metadata = DeviceGroupMetadata.from_dict(data)
            else:
                self._device_group_metadata = None

    def _save_metadata_file(self):
        if self._device_group_metadata is not None:
            log.info('Saving configuration data')
            data = self._device_group_metadata.to_dict()
            self._configurations_metadata_file.write_text(json.dumps(data, indent=2))

    def _remove_old_configurations(self):
        log.info('Removing old configurations')
        if self._device_group_metadata is None:
            log.error('Unable to remove old configurations without device group metadata')
            return

        valid_paths = {config.path for config in self._device_group_metadata.configurations_metadata}
        if self._device_group_metadata is not None:
            for file in chain(self._configurations_directory.glob('**/*'), self._configurations_directory.glob('*')):
                try:
                    rel_path = file.relative_to(self._configurations_directory)
                    if file.is_file() and str(rel_path) not in valid_paths:
                        try:
                            log.debug(f'Deleting file "{file}"')
                            file.unlink()
                        except FileNotFoundError:
                            log.warning(f'Could not delete file "{file}"')
                            # Do nothing
                            pass
                except ValueError:
                    log.error(f'Could not understand path for file "{file}"')

            remove_subdirs_if_empty(self._configurations_directory)

    def outdated_configurations(self) -> Generator[ConfigurationMetadata, None, None]:
        for config in sorted(self._device_group_metadata.configurations_metadata, key=lambda x: x.path):
            if config.md5 != file_md5(self._configurations_directory.joinpath(config.path)):
                yield config

    def is_outdated(self) -> bool:
        for f in self.outdated_configurations():
            log.info(f'OUTDATED: {f}')
            return True
        return False

    def check_latest(self) -> bool:
        log.info('Checking for latest configuration data')
        r: HTTPResponse = self._pool.request('GET', Client.BASE_URL + 'v1/current_configurations')
        if r.status == 200:
            data = json.loads(r.data.decode('utf-8'))
            self._device_group_metadata = DeviceGroupMetadata.from_server(data)
            self._save_metadata_file()
            self._state = State.MetadataDownloaded
            return True
        else:
            log.error(f'Failed to fetch latest configuration data: {r.status}: "{r.data.decode("utf-8")}"')
            self._state = State.Invalid_FailedToLoadMetadata
            self._device_group_metadata = None
            return False

    def download_configurations(self) -> bool:
        if self._device_group_metadata is None and not self.check_latest():
            return False

        self._remove_old_configurations()

        all_ok = True
        for config in self.outdated_configurations():
            path = self._configurations_directory.joinpath(config.path)
            path.parent.mkdir(exist_ok=True)
            with path.open('wb') as fp:
                r = self._pool.request('GET', Client.BASE_URL + 'v1/configuration', fields={
                    'configuration_id': config.configuration_id,
                    'version': config.version
                })
                if r.status == 200:
                    fp.write(r.data)
                else:
                    log.error((f'Failed to get configuration "{config.configuration_id}" version {config.version}. '
                               f'Received response {r.status}: "{r.data.decode("utf-8")}"'))
                    self._state = State.Invalid_FailedToDownload
                    all_ok = False
                    break

        if all_ok:
            log.info('Configurations downloaded')
            self._state = State.Valid
            return True
        else:
            return False

    def update_state(self, device_state: DeviceState) -> bool:
        if self._device_group_metadata is None:
            log.error(f'Cannot update state with "{device_state.name}" without first getting configuration data')
            return False

        log.info(f'Updating device state with "{device_state.name}"')
        r = self._pool.request('POST', Client.BASE_URL + 'v1/update_state',
                               headers={'Content-Type': 'application/json'},
                               body=json.dumps({
                                   'device_group_id': str(self._device_group_metadata.device_group_id),
                                   'device_group_version': self._device_group_metadata.device_group_version,
                                   'action': device_state.name,
                               }).encode('utf-8'))
        if r.status == 200:
            return True
        else:
            log.error(f'Failed to update state with "{device_state.name}". '
                      f'Received response {r.status}: "{r.data.decode("utf-8")}"')
            return False
