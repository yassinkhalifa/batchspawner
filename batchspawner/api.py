import json

from jupyterhub.apihandlers import APIHandler, default_handlers
from tornado import web

# Spawner attributes a single-user server may set through this API. Anything else
# (notably `ip`, which the Hub proxies to) must not be writable with a user token.
SETTABLE_KEYS = {"port"}


def validate_spawner_update(data):
    """Validate a POSTed spawner update and return the cleaned dict.

    Raises ValueError with a message suitable for a 400 response.
    """
    if not isinstance(data, dict):
        raise ValueError("expected a JSON object")
    unknown = set(data) - SETTABLE_KEYS
    if unknown:
        raise ValueError(f"cannot set: {', '.join(sorted(unknown))}")
    cleaned = {}
    if "port" in data:
        port = data["port"]
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("port must be an integer between 1 and 65535")
        cleaned["port"] = port
    return cleaned


class BatchSpawnerAPIHandler(APIHandler):
    @web.authenticated
    def post(self):
        """POST set user spawner data"""
        if hasattr(self, "current_user"):
            # Jupyterhub compatability, (september 2018, d79a99323ef1d)
            user = self.current_user
        else:
            # Previous jupyterhub, 0.9.4 and before.
            user = self.get_current_user()
        token = self.get_auth_token()
        spawner = None
        for s in user.spawners.values():
            if s.api_token == token:
                spawner = s
                break
        if spawner is None:
            raise web.HTTPError(404, "No spawner found for this token")
        try:
            data = validate_spawner_update(self.get_json_body())
        except ValueError as e:
            raise web.HTTPError(400, str(e))
        for key, value in data.items():
            setattr(spawner, key, value)
        self.finish(json.dumps({"message": "BatchSpawner data configured"}))


default_handlers.append((r"/api/batchspawner", BatchSpawnerAPIHandler))
