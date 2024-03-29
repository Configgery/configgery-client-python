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

from configgery.client import Client, DeviceGroupMetadata, ConfigurationMetadata, ClientState
from tests.FakeHTTPResponse import FakeHTTPResponse


@pytest.fixture
def configuration_directory() -> Path:
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def default_device_group_metadata(last_checked: Optional[datetime] = None):
    return {
        "device_group_id": "85ffb504-cc91-4710-a0e7-e05599b19d0b",
        "device_group_version": 1,
        "configurations_metadata": [
            {
                "configuration_id": "e312aa23-f8a8-4142-9a21-be640be7e547",
                "path": "foo.json",
                "md5": "99914b932bd37a50b983c5e7c90ae93b",
                "version": 1,
            },
            {
                "configuration_id": "85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a",
                "path": "bar.json",
                "md5": "3d29a75fcf0ed7dfff86d3db8f92fc69",
                "version": 2,
                "alias": "abc.json",
            },
        ],
        "version": 1,
        "last_checked": (last_checked or (datetime.now(tz=timezone.utc) - timedelta(days=1))).isoformat(),
    }


def write_metadata(configuration_directory, metadata):
    with configuration_directory.joinpath("configurations.json") as fp:
        fp.write_text(json.dumps(metadata, indent=2))


def all_files_and_dirs(d):
    return {f for f in chain(d.glob("**/*"), d.glob("*"))}


def test_init_no_previous_configurations(configuration_directory):
    c = Client("fake_api_key", configuration_directory)
    assert c._device_group_metadata is None


def test_init_with_configurations(configuration_directory):
    now = datetime.now(tz=timezone.utc)
    write_metadata(configuration_directory, default_device_group_metadata(last_checked=now))
    with freeze_time(now):
        c = Client("fake_api_key", configuration_directory)
    assert c._device_group_metadata is not None
    assert c._device_group_metadata == DeviceGroupMetadata(
        device_group_id=UUID("85ffb504-cc91-4710-a0e7-e05599b19d0b"),
        device_group_version=1,
        configurations_metadata={
            ConfigurationMetadata(
                configuration_id=UUID("e312aa23-f8a8-4142-9a21-be640be7e547"),
                path="foo.json",
                md5="99914b932bd37a50b983c5e7c90ae93b",
                version=1,
                alias=None,
            ),
            ConfigurationMetadata(
                configuration_id=UUID("85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a"),
                path="bar.json",
                md5="3d29a75fcf0ed7dfff86d3db8f92fc69",
                version=2,
                alias="abc.json",
            ),
        },
        last_checked=now,
    )


@pytest.mark.parametrize(
    (
        "version",
        "loaded",
    ),
    [
        (1, True),
        (2, False),
    ],
    ids=["validVersion", "invalidVersion"],
)
def test_init_with_wrong_file_version(configuration_directory, version, loaded):
    m = default_device_group_metadata()
    m["version"] = version
    write_metadata(configuration_directory, m)

    c = Client("fake_api_key", configuration_directory)
    if loaded:
        assert c._device_group_metadata is not None
    else:
        assert c._device_group_metadata is None


def test_init_with_corrupt_file(configuration_directory):
    m = default_device_group_metadata()
    del m["device_group_id"]
    write_metadata(configuration_directory, m)

    c = Client("fake_api_key", configuration_directory)
    assert c._device_group_metadata is None


def test_outdated_configurations(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configuration_directory.joinpath("configurations").mkdir()
    with configuration_directory.joinpath("configurations", m["configurations_metadata"][0]["path"]).open("wb") as fp:
        fp.write(b"{}")
    with configuration_directory.joinpath("configurations", m["configurations_metadata"][1]["path"]).open("wb") as fp:
        fp.write(b"invalid_data")

    c = Client("fake_api_key", configuration_directory)
    assert c.is_download_needed()
    outdated_configurations = list(c.outdated_configurations())
    assert len(outdated_configurations) == 1
    assert outdated_configurations[0].configuration_id == UUID(m["configurations_metadata"][1]["configuration_id"])


def test_remove_old_files_and_dirs(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configurations_dir = configuration_directory.joinpath("configurations")
    configurations_dir.mkdir()

    with configurations_dir.joinpath("a.json").open("wb") as fp:
        fp.write(b"hello world")

    configurations_dir.joinpath("dir1").mkdir()
    with configurations_dir.joinpath("dir1/a.json").open("wb") as fp:
        fp.write(b"hello world")

    with configurations_dir.joinpath("foo.json").open("wb") as fp:
        fp.write(b"{}")

    with configurations_dir.joinpath("bar.json").open("wb") as fp:
        fp.write(b"{\n}")

    configurations_dir.joinpath("dir1/dir2/dir3").mkdir(parents=True)

    c = Client("fake_api_key", configuration_directory)
    c._remove_old_configurations()

    assert all_files_and_dirs(configurations_dir) == {
        configurations_dir.joinpath("foo.json"),
        configurations_dir.joinpath("bar.json"),
    }


def test_must_identify_first(configuration_directory):
    c = Client("fake_api_key", configuration_directory)
    assert c._device_group_metadata is None

    with pytest.raises(ValueError):
        assert c.check_latest()

    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "id": "621a4632-0049-4cb7-b232-3db0c3d27ade",
                    },
                    indent=2,
                ).encode(),
            ),
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "device_group_id": "85ffb504-cc91-4710-a0e7-e05599b19d0b",
                        "device_group_version": 1,
                        "configurations": [],
                    },
                    indent=2,
                ).encode(),
            ),
        ]

        c.identify("my_client")
        assert c.check_latest()


def test_check_latest(configuration_directory):
    now = datetime.now(tz=timezone.utc)

    c = Client("fake_api_key", configuration_directory)
    assert c._device_group_metadata is None

    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "id": "621a4632-0049-4cb7-b232-3db0c3d27ade",
                    },
                    indent=2,
                ).encode(),
            ),
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "device_group_id": "85ffb504-cc91-4710-a0e7-e05599b19d0b",
                        "device_group_version": 1,
                        "configurations": [
                            {
                                "configuration_id": "e312aa23-f8a8-4142-9a21-be640be7e547",
                                "path": "foo.json",
                                "md5": "99914b932bd37a50b983c5e7c90ae93b",
                                "version": 1,
                            },
                            {
                                "configuration_id": "85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a",
                                "path": "bar.json",
                                "md5": "3d29a75fcf0ed7dfff86d3db8f92fc69",
                                "version": 2,
                                "alias": "abc.json",
                            },
                        ],
                    },
                    indent=2,
                ).encode(),
            ),
        ]

        c.identify("my_device")
        with freeze_time(now):
            assert c.check_latest()

    assert c._device_group_metadata == DeviceGroupMetadata(
        device_group_id=UUID("85ffb504-cc91-4710-a0e7-e05599b19d0b"),
        device_group_version=1,
        configurations_metadata={
            ConfigurationMetadata(
                configuration_id=UUID("e312aa23-f8a8-4142-9a21-be640be7e547"),
                path="foo.json",
                md5="99914b932bd37a50b983c5e7c90ae93b",
                version=1,
                alias=None,
            ),
            ConfigurationMetadata(
                configuration_id=UUID("85d0acae-4a9c-49ce-b8dc-f8a41c6c6c6a"),
                path="bar.json",
                md5="3d29a75fcf0ed7dfff86d3db8f92fc69",
                version=2,
                alias="abc.json",
            ),
        },
        last_checked=now,
    )

    with freeze_time(now + timedelta(hours=1)):
        assert c.time_since_last_checked() == timedelta(hours=1)


def test_download_new_configurations(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    configurations_dir = configuration_directory.joinpath("configurations")
    configurations_dir.mkdir()
    # Ensure old configurations are deleted
    with configurations_dir.joinpath("oldfile.json").open("wb") as fp:
        fp.write(b"hello world")

    c = Client("fake_api_key", configuration_directory)
    assert c.is_download_needed()
    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "id": "621a4632-0049-4cb7-b232-3db0c3d27ade",
                    },
                    indent=2,
                ).encode(),
            ),
            FakeHTTPResponse(status=200, data=b"{\n}"),
            FakeHTTPResponse(status=200, data=b"{}"),
        ]
        c.identify("my_device")
        assert c.download_configurations()

    assert all_files_and_dirs(configurations_dir) == {
        configurations_dir.joinpath("foo.json"),
        configurations_dir.joinpath("bar.json"),
    }
    download_needed = c.is_download_needed()
    if download_needed:
        print(download_needed)
    assert not download_needed


def test_make_parent_directories_for_configuration_metadata():
    with tempfile.TemporaryDirectory() as d:
        configuration_directory = Path(d).joinpath("a/b/c")
        assert not configuration_directory.exists()
        _ = Client("fake_api_key", configuration_directory)
        assert configuration_directory.exists()


def test_make_parent_directories_for_configuration_files(configuration_directory):
    m = default_device_group_metadata()
    m["configurations_metadata"].append(
        {
            "configuration_id": "2bfb6125-96fd-402f-a585-1799612bf9cc",
            "path": "a/b/c/d.json",
            "md5": "99914b932bd37a50b983c5e7c90ae93b",
            "version": 1,
        }
    )
    write_metadata(configuration_directory, m)

    c = Client("fake_api_key", configuration_directory)

    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "id": "621a4632-0049-4cb7-b232-3db0c3d27ade",
                    },
                    indent=2,
                ).encode(),
            ),
            FakeHTTPResponse(status=200, data=b"{}"),
            FakeHTTPResponse(status=200, data=b"{\n}"),
            FakeHTTPResponse(status=200, data=b"{}"),
        ]
        c.identify("my_device")
        assert c.download_configurations()

    configurations_dir = configuration_directory.joinpath("configurations")
    assert all_files_and_dirs(configurations_dir) == {
        configurations_dir.joinpath("a"),
        configurations_dir.joinpath("a/b"),
        configurations_dir.joinpath("a/b/c"),
        configurations_dir.joinpath("a/b/c/d.json"),
        configurations_dir.joinpath("foo.json"),
        configurations_dir.joinpath("bar.json"),
    }


def test_update_state(configuration_directory):
    m = default_device_group_metadata()
    write_metadata(configuration_directory, m)

    c = Client("fake_api_key", configuration_directory)
    with MagicMock() as mock_poolmanager:
        c._pool = mock_poolmanager
        mock_poolmanager.request.side_effect = [
            FakeHTTPResponse(
                status=200,
                data=json.dumps(
                    {
                        "id": "621a4632-0049-4cb7-b232-3db0c3d27ade",
                    },
                    indent=2,
                ).encode(),
            ),
            FakeHTTPResponse(status=200, data=b"OK"),
        ]
        c.identify("my_device")
        assert c.update_state(ClientState.Configurations_Applied)


def test_update_state_fails_without_cached_configuration_data(configuration_directory):
    c = Client("fake_api_key", configuration_directory)
    assert not c.update_state(ClientState.Configurations_Applied)
