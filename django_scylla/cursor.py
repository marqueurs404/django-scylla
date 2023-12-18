import logging
from enum import Enum
from typing import Optional

from cassandra.cluster import ResultSet, Session
from cassandra.util import Date, SortedSet

logger = logging.getLogger(__name__)


def list_factory(colnames, rows):
    for j in range(len(rows)):
        rows[j] = list(rows[j])
        for i in range(len(rows[j])):
            if isinstance(rows[j][i], Date):
                rows[j][i] = rows[j][i].date()
            elif isinstance(rows[j][i], SortedSet):
                rows[j][i] = set(rows[j][i])
        rows[j] = tuple(rows[j])
    return rows


class Cursor:
    def __init__(self, session: Session):
        logger.debug("CURSOR: Initialize Cursor")
        self.session = session
        self.result: Optional[ResultSet] = None

    def __del__(self):
        logger.debug("CURSOR: shutdown session")
        self.session.shutdown()

    def close(self):
        ...

    def __iter__(self):
        return iter(self.result)

    @property
    def keyspace(self):
        return self.session.keyspace

    @property
    def rowcount(self):
        if self.result is None:
            raise RuntimeError

        # TODO: possibly not optimal
        return len(self.result.all())

    def set_keyspace(self, name: str):
        return self.session.set_keyspace(name)

    def execute(self, query: str, parameters=None):
        if not query:
            return None
        logger.debug("QUERY %s, params %s", query, parameters)

        if parameters:
            for i in range(len(parameters)):
                if isinstance(parameters[i], Enum):
                    parameters[i] = str(parameters[i])

        self.result = self.session.execute(query, parameters=parameters)
        return self.result

    def fetchmany(self, size=1):
        if size == 1:
            return self.fetchone()
        res = self.result.all()[:size]
        return res

    def fetchone(self):
        res = self.result.one()
        is_empty = len(res) == 1 and res[0] == 0
        return None if is_empty else res

    def fetchall(self):
        return self.result.all()

    @property
    def lastrowid(self):
        ...
