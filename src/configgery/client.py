from __future__ import annotations

import json
import logging
import os
from enum import Enum, auto
from itertools import chain
from pathlib import Path
from typing import Optional, Generator

from urllib3 import PoolManager, HTTPResponse

from .configurations_metadata import DeviceGroupMetadata, load_metadata_file, save_metadata_file, ConfigurationMetadata
from .file import file_md5, remove_subdirs_if_empty

log = logging.getLogger(__name__)


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
            self._device_group_metadata = load_metadata_file(self._configurations_metadata_file)
        else:
            log.info('No cached configuration data found')

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
        for _ in self.outdated_configurations():
            return True
        return False

    def check_latest(self) -> bool:
        log.info('Checking for latest configuration data')
        r: HTTPResponse = self._pool.request('GET', Client.BASE_URL + 'v1/current_configurations')
        if r.status == 200:
            data = json.loads(r.data.decode('utf-8'))
            self._device_group_metadata = DeviceGroupMetadata.from_server(data)
            save_metadata_file(self._device_group_metadata, self._configurations_metadata_file)
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

        if hasattr(os, 'sync'):
            os.sync()

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
