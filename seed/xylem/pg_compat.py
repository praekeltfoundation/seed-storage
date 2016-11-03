# Handle differences between psycopg2 and psycopg2cffi in one place.

try:
    # Partly to keep flake8 happy, partly to support psycopg2.
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass

import psycopg2
from psycopg2 import errorcodes

__all__ = ['psycopg2', 'errorcodes']
