#!/usr/bin/env python

import socket

from .decorator import timeout


@timeout
def closed(port, host='localhost', timeout=300):
    try:
        s = socket.create_connection((host, port))
        s.close()
    except OSError:
        return True
    return False


@timeout
def open(port, host='localhost', timeout=300):
    try:
        s = socket.create_connection((host, port))
        s.close()
    except OSError:
        return False
    return True
