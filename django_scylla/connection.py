import logging

logger = logging.getLogger(__name__)


class ConnectionWrapper:
    def __init__(self, session):
        self.session = session

    def close(self):
        ...

    @property
    def keyspace(self):
        return self.session.keyspace

    def set_keyspace(self, name):
        return self.session.set_keyspace(name)

    def execute(self, query, parameters=None):
        logger.debug(f"QUERY: {query}, {parameters}")
        return self.session.execute(query, parameters=parameters)
