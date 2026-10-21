"""Relevant pytest fixtures are re-used from JupyterHub's test suite"""

from jupyterhub.tests.conftest import db  # noqa

try:  # older JupyterHub / pytest-asyncio provide an event_loop fixture
    from jupyterhub.tests.conftest import event_loop  # noqa
except ImportError:
    pass
