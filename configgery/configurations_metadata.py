from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple, Optional, Any, Dict, Set
from uuid import UUID

log = logging.getLogger(__name__)
CURRENT_CONFIG_FILE_VERSION = 1


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
            'version': CURRENT_CONFIG_FILE_VERSION,
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


def load_metadata_file(file: Path) -> Optional[DeviceGroupMetadata]:
    try:
        with file.open('r') as fp:
            data = json.load(fp)
    except (FileNotFoundError, PermissionError, json.JSONDecodeError):
        log.exception(f'Unable to read cached configuration data')
        return None
    else:
        if data['version'] != CURRENT_CONFIG_FILE_VERSION:
            log.warning(f'Invalid file version {data["version"]}')
            return None
        elif 'device_group_id' in data:
            return DeviceGroupMetadata.from_dict(data)
        else:
            return None


def save_metadata_file(metadata: DeviceGroupMetadata, file: Path):
    if metadata is not None:
        log.info('Saving configuration data')
        data = metadata.to_dict()
        file.write_text(json.dumps(data, indent=2))
