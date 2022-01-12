import logging
from binascii import hexlify
from hashlib import md5
from pathlib import Path

log = logging.getLogger(__name__)


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
