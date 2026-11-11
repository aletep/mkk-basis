import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from mkk_basis.adapters.postgres import Base
from mkk_basis.bootstrap.config import Settings


async def online() -> None:
    engine = create_async_engine(Settings().database_url.get_secret_value())
    async with engine.connect() as connection:

        def migrate(sync_connection: object) -> None:
            context.configure(connection=sync_connection, target_metadata=Base.metadata)
            with context.begin_transaction():
                context.run_migrations()

        await connection.run_sync(migrate)
    await engine.dispose()


if context.is_offline_mode():
    context.configure(url=Settings().database_url.get_secret_value(), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(online())
