import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
from freezegun import freeze_time

from src.configgery.client import Client, DeviceGroupMetadata, ConfigurationMetadata


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


def test_init_no_previous_configurations(configuration_directory, certificate, private_key):
    c = Client(configuration_directory, certificate, private_key)
    assert c.device_group_metadata is None


def test_init_with_configurations(configuration_directory, certificate, private_key):
    with configuration_directory.joinpath('configurations.json') as fp:
        fp.write_text(json.dumps(default_device_group_metadata(), indent=2))

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
    with configuration_directory.joinpath('configurations.json') as fp:
        fp.write_text(json.dumps(m, indent=2))

    c = Client(configuration_directory, certificate, private_key)
    if loaded:
        assert c.device_group_metadata is not None
    else:
        assert c.device_group_metadata is None


def test_init_with_corrupt_file(configuration_directory, certificate, private_key):
    m = default_device_group_metadata()
    del m['device_group_id']
    with configuration_directory.joinpath('configurations.json') as fp:
        fp.write_text(json.dumps(m, indent=2))

    c = Client(configuration_directory, certificate, private_key)
    assert c.device_group_metadata is None
