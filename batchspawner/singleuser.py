import os
import sys
import time
from runpy import run_path
from shutil import which
from urllib.parse import urlparse, urlunparse

import requests
from jupyterhub.services.auth import HubAuth
from jupyterhub.utils import random_port, url_path_join


REGISTER_ATTEMPTS = 6
REGISTER_TIMEOUT = 10  # seconds per request
REGISTER_MAX_DELAY = 30  # seconds between attempts, at most


def register_port(url, headers, port, attempts=REGISTER_ATTEMPTS, **kwargs):
    """Tell the Hub which port this server listens on, retrying transient failures.

    Without this the Hub waits for the port until its start timeout while the job
    keeps running, so a brief Hub outage must not be fatal. Client errors other than
    429 are not retried since they will not fix themselves.

    Raises requests.RequestException once all attempts are used up.
    """
    delay = 1
    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json={"port": port},
                timeout=REGISTER_TIMEOUT,
                **kwargs,
            )
            response.raise_for_status()
            return
        except requests.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            permanent = status is not None and 400 <= status < 500 and status != 429
            if permanent or attempt == attempts:
                raise
            print(
                f"batchspawner-singleuser: registering port failed ({e}); "
                f"retrying in {delay}s (attempt {attempt} of {attempts})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
            delay = min(delay * 2, REGISTER_MAX_DELAY)


def main(argv=None):
    port = random_port()
    hub_auth = HubAuth()

    url = url_path_join(hub_auth.api_url, "batchspawner")
    headers = {"Authorization": f"token {hub_auth.api_token}"}

    # internal_ssl kwargs
    kwargs = {}
    if hub_auth.certfile and hub_auth.keyfile:
        kwargs["cert"] = (hub_auth.certfile, hub_auth.keyfile)
    if hub_auth.client_ca:
        kwargs["verify"] = hub_auth.client_ca

    try:
        register_port(url, headers, port, **kwargs)
    except requests.RequestException as e:
        # Exit so the job ends and the Hub reports a failed start promptly, instead of
        # leaving a server running that the Hub can never reach.
        print(f"batchspawner-singleuser: could not register port {port} with the Hub: {e}", file=sys.stderr)
        sys.exit(1)

    # Read the env var JUPYTERHUB_SERVICE_URL and replace port in the URL
    # with free port that we found here
    # JUPYTERHUB_SERVICE_URL is added in JupyterHub 2.0
    service_url_env = os.environ.get("JUPYTERHUB_SERVICE_URL", "")
    if service_url_env:
        url = urlparse(os.environ["JUPYTERHUB_SERVICE_URL"])
        url = url._replace(netloc=f"{url.hostname}:{port}")
        os.environ["JUPYTERHUB_SERVICE_URL"] = urlunparse(url)
    else:
        # JupyterHub < 2.0 specifies port on the command-line
        sys.argv.append(f"--port={port}")

    cmd_path = which(sys.argv[1])
    sys.argv = sys.argv[1:]
    run_path(cmd_path, run_name="__main__")


if __name__ == "__main__":
    main()
