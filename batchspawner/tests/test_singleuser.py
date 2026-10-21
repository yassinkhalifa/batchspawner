"""Test registering the single-user port with the Hub"""

from unittest import mock

import pytest
import requests

from .. import singleuser


def response(status):
    r = requests.Response()
    r.status_code = status
    return r


@pytest.fixture(autouse=True)
def no_sleep():
    with mock.patch.object(singleuser.time, "sleep") as sleep:
        yield sleep


def test_registers_port_once_on_success():
    with mock.patch.object(singleuser.requests, "post", return_value=response(200)) as post:
        singleuser.register_port("http://hub/api/batchspawner", {"Authorization": "token x"}, 4242)

    assert post.call_count == 1
    assert post.call_args.kwargs["json"] == {"port": 4242}
    assert post.call_args.kwargs["timeout"] == singleuser.REGISTER_TIMEOUT


def test_retries_connection_errors_then_succeeds(no_sleep):
    side_effect = [requests.ConnectionError("hub down"), requests.Timeout("slow"), response(200)]
    with mock.patch.object(singleuser.requests, "post", side_effect=side_effect) as post:
        singleuser.register_port("http://hub/api/batchspawner", {}, 4242)

    assert post.call_count == 3
    assert no_sleep.call_count == 2


def test_retries_server_errors():
    with mock.patch.object(singleuser.requests, "post", side_effect=[response(503), response(200)]) as post:
        singleuser.register_port("http://hub/api/batchspawner", {}, 4242)

    assert post.call_count == 2


def test_gives_up_after_all_attempts():
    with mock.patch.object(singleuser.requests, "post", side_effect=requests.ConnectionError("down")) as post:
        with pytest.raises(requests.ConnectionError):
            singleuser.register_port("http://hub/api/batchspawner", {}, 4242, attempts=3)

    assert post.call_count == 3


@pytest.mark.parametrize("status", [400, 403, 404])
def test_does_not_retry_client_errors(status):
    with mock.patch.object(singleuser.requests, "post", return_value=response(status)) as post:
        with pytest.raises(requests.HTTPError):
            singleuser.register_port("http://hub/api/batchspawner", {}, 4242)

    assert post.call_count == 1


def test_retries_rate_limiting():
    with mock.patch.object(singleuser.requests, "post", side_effect=[response(429), response(200)]) as post:
        singleuser.register_port("http://hub/api/batchspawner", {}, 4242)

    assert post.call_count == 2
