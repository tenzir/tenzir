#!/usr/bin/env python

import time


def timeout(func):
    def _timeout(*args, **kwargs):
        TIMEOUT = 300

        if 'timeout' in kwargs:
            TIMEOUT = kwargs['timeout']

        start = time.time()

        while True:
            if func(*args, **kwargs):
                return True

            time.sleep(0.1)

            if TIMEOUT:
                if time.time() - start > TIMEOUT:
                    return False

    return _timeout


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
