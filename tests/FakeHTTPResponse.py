from dataclasses import dataclass


@dataclass
class FakeHTTPResponse:
    status: int
    data: bytes
