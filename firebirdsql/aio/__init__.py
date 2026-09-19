import asyncio
from typing import Any
from firebirdsql.consts import DEFAULT_CHARSET
from .fbcore import AsyncConnection, AsyncCursor
from .pool import create_pool


async def connect(
    dsn: str | None = None,
    user: str | None = None,
    password: str | None = None,
    role: str | None = None,
    host: str | None = None,
    database: str | None = None,
    charset: str = DEFAULT_CHARSET,
    port: int | None = None,
    page_size: int = 4096,
    is_services: bool = False,
    cloexec: bool = False,
    timeout: float | None = None,
    isolation_level: int | None = None,
    auth_plugin_name: str | None = None,
    wire_crypt: bool = True,
    create_new: bool = False,
    timezone: str | None = None,
    wire_compress: bool = False,
    readonly: bool = False,
    loop: asyncio.AbstractEventLoop | None = None,
    **kwargs: Any,
) -> AsyncConnection:
    conn = AsyncConnection(
        dsn=dsn,
        user=user,
        password=password,
        role=role,
        host=host,
        database=database,
        charset=charset,
        port=port,
        page_size=page_size,
        is_services=is_services,
        cloexec=cloexec,
        timeout=timeout,
        isolation_level=isolation_level,
        auth_plugin_name=auth_plugin_name,
        wire_crypt=wire_crypt,
        create_new=create_new,
        timezone=timezone,
        wire_compress=wire_compress,
        readonly=readonly,
        loop=loop,
        **kwargs,
    )
    await conn._initialize()
    return conn

