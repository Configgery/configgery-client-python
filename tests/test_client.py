import json
import tempfile
from datetime import datetime, timezone, timedelta
from itertools import chain
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from freezegun import freeze_time

from src.configgery.client import Client, DeviceGroupMetadata, ConfigurationMetadata
from tests.FakeHTTPResponse import FakeHTTPResponse


@pytest.fixture
def configuration_directory() -> Path:
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def default_device_group_metadata(last_checked: Optional[datetime] = None):
    return {
        'device_group_id': '85ffb504-cc91-4710-a0e7-e05599b19d0b',
        'device_group_version': 1,
        'configurations_metadata': [
            {
                'configuration_id': 'e312aa23-f8a8-4142-9a21-be640be7e547',
                'path': 'foo.json',
                'md5': '99914b932bd37a50b983c5e7c90ae93b',
                'version': 1,
            },
            {
                'configuration_id': '85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a',
                'path': 'bar.json',
                'md5': '3d29a75fcf0ed7dfff86d3db8f92fc69',
                'version': 2,
                'alias': 'abc.json',
            },
        ],
        'version': 1,
        'last_checked': (last_checked or (datetime.now(tz=timezone.utc) - timedelta(days=1))).isoformat(),
    }


def write_metadata(configuration_directory, metadata):
    with configuration_directory.joinpath('configurations.json') as fp:
        fp.write_text(json.dumps(metadata, indent=2))


def all_files_and_dirs(d):
    return {f for f in chain(d.glob('**/*'), d.glob('*'))}


def test_init_no_previous_configurations(configuration_directory):
    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    assert c._device_group_metadata is None


def test_init_with_configurations(configuration_directory):
    now = datetime.now(tz=timezone.utc)
    write_metadata(configuration_directory, default_device_group_metadata(last_checked=now))
    with freeze_time(now):
        c = Client(configuration_directory, Path('/cert'), Path('/key'))
    assert c._device_group_metadata is not None
    assert c._device_group_metadata == DeviceGroupMetadata(
        device_group_id=UUID('85ffb504-cc91-4710-a0e7-e05599b19d0b'),
        device_group_version=1,
        configurations_metadata={
            ConfigurationMetadata(
                configuration_id=UUID('e312aa23-f8a8-4142-9a21-be640be7e547'),
                path='foo.json',
                md5='99914b932bd37a50b983c5e7c90ae93b',
                version=1,
                alias=None,
            ),
            ConfigurationMetadata(
                configuration_id=UUID('85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a'),
                path='bar.json',
                md5='3d29a75fcf0ed7dfff86d3db8f92fc69',
                version=2,
                alias='abc.json',
            ),
        },
        last_checked=now
    )


@pytest.mark.parametrize(
    ('version', 'loaded',),
    [
        (1, True),
        (2, False),
    ],
    ids=['validVersion', 'invalidVersion']
)
def test_init_with_wrong_file_version(configuration_directory, version, loaded):
    m = default_device_group_metadata()
    m['version'] = version
    write_metadata(configuration_directory, m)

    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    if loaded:
        assert c._device_group_metadata is not None
    else:
        assert c._device_group_metadata is None


def test_init_with_corrupt_file(configuration_directory):
    m = default_device_group_metadata()
    del m['device_group_id']
    write_metadata(configuration_directory, m)

    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    assert c._device_group_metadata is None


def test_outdated_configurations(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configuration_directory.joinpath('configurations').mkdir()
    with configuration_directory.joinpath('configurations', m['configurations_metadata'][0]['path']).open('wb') as fp:
        fp.write(b'{}')
    with configuration_directory.joinpath('configurations', m['configurations_metadata'][1]['path']).open('wb') as fp:
        fp.write(b'invalid_data')

    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    assert c.is_outdated()
    outdated_configurations = list(c.outdated_configurations())
    assert len(outdated_configurations) == 1
    assert outdated_configurations[0].configuration_id == UUID(m['configurations_metadata'][1]['configuration_id'])


def test_remove_old_files_and_dirs(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configurations_dir = configuration_directory.joinpath('configurations')
    configurations_dir.mkdir()

    with configurations_dir.joinpath('a.json').open('wb') as fp:
        fp.write(b'hello world')

    configurations_dir.joinpath('dir1').mkdir()
    with configurations_dir.joinpath('dir1/a.json').open('wb') as fp:
        fp.write(b'hello world')

    with configurations_dir.joinpath('foo.json').open('wb') as fp:
        fp.write(b'{}')

    with configurations_dir.joinpath('bar.json').open('wb') as fp:
        fp.write(b'{\n}')

    configurations_dir.joinpath('dir1/dir2/dir3').mkdir(parents=True)

    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    c._remove_old_configurations()

    assert all_files_and_dirs(configurations_dir) == {
        configurations_dir.joinpath('foo.json'),
        configurations_dir.joinpath('bar.json'),
    }


def test_check_latest(configuration_directory):
    now = datetime.now(tz=timezone.utc)

    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    assert c._device_group_metadata is None

    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps({
                    'device_group_id': '85ffb504-cc91-4710-a0e7-e05599b19d0b',
                    'device_group_version': 1,
                    'configurations': [
                        {
                            'configuration_id': 'e312aa23-f8a8-4142-9a21-be640be7e547',
                            'path': 'foo.json',
                            'md5': '99914b932bd37a50b983c5e7c90ae93b',
                            'version': 1,
                        },
                        {
                            'configuration_id': '85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a',
                            'path': 'bar.json',
                            'md5': '3d29a75fcf0ed7dfff86d3db8f92fc69',
                            'version': 2,
                            'alias': 'abc.json',
                        },
                    ],
                }, indent=2).encode()
            )
        ]

        with freeze_time(now):
            assert c.check_latest()

    assert c._device_group_metadata == DeviceGroupMetadata(
        device_group_id=UUID('85ffb504-cc91-4710-a0e7-e05599b19d0b'),
        device_group_version=1,
        configurations_metadata={
            ConfigurationMetadata(
                configuration_id=UUID('e312aa23-f8a8-4142-9a21-be640be7e547'),
                path='foo.json',
                md5='99914b932bd37a50b983c5e7c90ae93b',
                version=1,
                alias=None,
            ),
            ConfigurationMetadata(
                configuration_id=UUID('85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a'),
                path='bar.json',
                md5='3d29a75fcf0ed7dfff86d3db8f92fc69',
                version=2,
                alias='abc.json',
            ),
        },
        last_checked=now
    )


def test_download_new_configurations(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configurations_dir = configuration_directory.joinpath('configurations')
    configurations_dir.mkdir()
    # Ensure old configurations are deleted
    with configurations_dir.joinpath('oldfile.json').open('wb') as fp:
        fp.write(b'hello world')

    c = Client(configuration_directory, Path('/cert'), Path('/key'))
    assert c.is_outdated()
    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=b'{}'
            ),
            FakeHTTPResponse(
                status=200,
                data=b'{\n}'
            ),
        ]
        assert c.download_configurations()

    assert all_files_and_dirs(configurations_dir) == {
        configurations_dir.joinpath('foo.json'),
        configurations_dir.joinpath('bar.json'),
    }
    assert not c.is_outdated()
