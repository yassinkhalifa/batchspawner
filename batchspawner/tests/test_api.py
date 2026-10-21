"""Test validation of the data a single-user server may set on its spawner"""

import pytest

from ..api import validate_spawner_update


def test_valid_port():
    assert validate_spawner_update({"port": 54321}) == {"port": 54321}


def test_empty_update_is_allowed():
    assert validate_spawner_update({}) == {}


@pytest.mark.parametrize("key", ["ip", "cmd", "job_id", "batch_script", "user_options"])
def test_other_attributes_cannot_be_set(key):
    with pytest.raises(ValueError):
        validate_spawner_update({key: "10.0.0.1"})


def test_port_cannot_be_combined_with_other_attributes():
    with pytest.raises(ValueError):
        validate_spawner_update({"port": 54321, "ip": "10.0.0.1"})


@pytest.mark.parametrize("port", [0, -1, 65536, "8080", 80.5, None, True, [8080]])
def test_invalid_port(port):
    with pytest.raises(ValueError):
        validate_spawner_update({"port": port})


@pytest.mark.parametrize("data", [None, [], "port", 5])
def test_body_must_be_an_object(data):
    with pytest.raises(ValueError):
        validate_spawner_update(data)
