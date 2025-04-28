from .fbcore import AsyncConnection


async def connect(**kwargs):
    conn = AsyncConnection(**kwargs)
    await conn._initialize_socket()
    return conn
