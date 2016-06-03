# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

__version__ = "0.5.4"

from .connection import Connection
from .const import (
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    ENCODING_DEFAULT
)

from .error import (
    Error,
    DatabaseError,
    NetworkError,
    NetworkWarning,
    RetryWarning
)

from .schema import (
    Schema,
    SchemaError
)


def connect(host="localhost", port=33013, user=None, password=None, encoding=ENCODING_DEFAULT):
    '''\
    Create a connection to the Tarantool server.

    :param str host: Server hostname or IP-address
    :param int port: Server port

    :rtype: :class:`~tarantool.connection.Connection`

    :raise: `NetworkError`
    '''

    return Connection(host, port,
                      user=user,
                      password=password,
                      socket_timeout=SOCKET_TIMEOUT,
                      reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                      reconnect_delay=RECONNECT_DELAY,
                      connect_now=True,
                      encoding=encoding)


__all__ = ['connect', 'Connection', 'Schema', 'Error', 'DatabaseError',
           'NetworkError', 'NetworkWarning', 'RetryWarning', 'SchemaError']
