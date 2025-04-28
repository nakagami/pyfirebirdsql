from .fbcore import AsyncConnection, AsyncCursor
from .pool import create_pool


async def connect(**kwargs):
    conn = AsyncConnection(**kwargs)
    await conn._initialize_socket()
    return conn
