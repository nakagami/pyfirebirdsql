import asyncio
from .fbcore import AsyncConnection, AsyncCursor
from .pool import create_pool


def connect(**kwargs):
    conn = AsyncConnection(**kwargs)
    conn._initialize_socket()
    return conn
