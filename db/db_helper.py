from asyncio import current_task

from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
    async_scoped_session,
)

from config import settings


class DataBaseHelper:
    def __init__(self, url):
        self.engine = create_async_engine(url, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

    def get_scoped_session(self):
        session = async_scoped_session(
            session_factory=self.session_factory,
            scopefunc=current_task,
        )
        return session

    async def scoped_session_dependency(self):
        scoped_factory = self.get_scoped_session()
        session = scoped_factory()
        try:
            yield session
        finally:
            await session.close()
            await scoped_factory.remove()


db_helper = DataBaseHelper(settings.database_url)
