import json
import tempfile
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path
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


@pytest.fixture
def certificate() -> Path:
    with tempfile.NamedTemporaryFile() as f:
        f.write(b'')
        yield Path(f.name)


@pytest.fixture
def private_key() -> Path:
    with tempfile.NamedTemporaryFile() as f:
        f.write(b'')
        yield Path(f.name)


def default_device_group_metadata():
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
        'version': 1
    }


def write_metadata(configuration_directory, metadata):
    with configuration_directory.joinpath('configurations.json') as fp:
        fp.write_text(json.dumps(metadata, indent=2))


def all_files_and_dirs(d):
    return {f for f in chain(d.glob('**/*'), d.glob('*'))}


def test_init_no_previous_configurations(configuration_directory, certificate, private_key):
    c = Client(configuration_directory, certificate, private_key)
    assert c.device_group_metadata is None


def test_init_with_configurations(configuration_directory, certificate, private_key):
    write_metadata(configuration_directory, default_device_group_metadata())

    now = datetime.now(tz=timezone.utc)
    with freeze_time(now):
        c = Client(configuration_directory, certificate, private_key)
    assert c.device_group_metadata is not None
    assert c.device_group_metadata == DeviceGroupMetadata(
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
        last_loaded=now
    )


@pytest.mark.parametrize(
    ('version', 'loaded',),
    [
        (1, True),
        (2, False),
    ],
    ids=['validVersion', 'invalidVersion']
)
def test_init_with_wrong_file_version(configuration_directory, certificate, private_key, version, loaded):
    m = default_device_group_metadata()
    m['version'] = version
    write_metadata(configuration_directory, m)

    c = Client(configuration_directory, certificate, private_key)
    if loaded:
        assert c.device_group_metadata is not None
    else:
        assert c.device_group_metadata is None


def test_init_with_corrupt_file(configuration_directory, certificate, private_key):
    m = default_device_group_metadata()
    del m['device_group_id']
    write_metadata(configuration_directory, m)

    c = Client(configuration_directory, certificate, private_key)
    assert c.device_group_metadata is None


def test_outdated_configurations(configuration_directory, certificate, private_key):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configuration_directory.joinpath('configurations').mkdir()
    with configuration_directory.joinpath('configurations', m['configurations_metadata'][0]['path']).open('wb') as fp:
        fp.write(b'{}')
    with configuration_directory.joinpath('configurations', m['configurations_metadata'][1]['path']).open('wb') as fp:
        fp.write(b'invalid_data')

    c = Client(configuration_directory, certificate, private_key)
    outdated_configurations = list(c.outdated_configurations())
    assert len(outdated_configurations) == 1
    assert outdated_configurations[0].configuration_id == UUID(m['configurations_metadata'][1]['configuration_id'])


def test_remove_old_files_and_dirs(configuration_directory, certificate, private_key):
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

    c = Client(configuration_directory, certificate, private_key)
    c._remove_old_configurations()

    assert all_files_and_dirs(configurations_dir) == {
        configurations_dir.joinpath('foo.json'),
        configurations_dir.joinpath('bar.json'),
    }


def test_load_device_group_metadata(configuration_directory, certificate, private_key):
    now = datetime.now(tz=timezone.utc)

    c = Client(configuration_directory, certificate, private_key)
    assert c.device_group_metadata is None

    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps({
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
                }, indent=2).encode()
            )
        ]

        with freeze_time(now):
            c.load_device_group_metadata()

    assert c.device_group_metadata == DeviceGroupMetadata(
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
        last_loaded=now
    )


def test_download_new_configurations(configuration_directory, certificate, private_key):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configurations_dir = configuration_directory.joinpath('configurations')
    configurations_dir.mkdir()
    with configurations_dir.joinpath('oldfile.json').open('wb') as fp:
        fp.write(b'hello world')

    c = Client(configuration_directory, certificate, private_key)
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
