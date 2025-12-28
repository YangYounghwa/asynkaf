from . import _core


class Consumer:
    def __init__(self, bootstrap_servers: str, group_id: str):
        self._consumer = _core.create_consumer(bootstrap_servers, group_id)