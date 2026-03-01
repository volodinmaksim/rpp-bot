from sqlalchemy import select, update

from db.db_helper import db_helper
from db.models import User, Events
from exception.db import UserNotFound


async def add_user(tg_id: int, username: str, utm_mark: str):
    async with db_helper.session_factory() as session:
        user = await get_user(tg_id)

        if not user:
            new_user = User(tg_id=tg_id, utm_mark=utm_mark, username=username)
            session.add(new_user)
            await session.commit()


async def get_user(tg_id: int):
    async with db_helper.session_factory() as session:
        query = select(User).where(User.tg_id == tg_id)
        result = await session.execute(query)
        return result.scalar_one_or_none()


async def add_event(tg_id: int, event_name: str):
    async with db_helper.session_factory() as session:
        user_query = await session.execute(
            select(User.id).where(User.tg_id == tg_id),
        )
        internal_user_id = user_query.scalar()

        if internal_user_id:
            new_event = Events(user_id=internal_user_id, event_name=event_name)
            session.add(new_event)
            await session.commit()
        else:
            raise UserNotFound


async def set_segment(tg_id: int, segment: str):
    async with db_helper.session_factory() as session:
        stmt = update(User).where(User.tg_id == tg_id).values(segment=segment)
        await session.execute(stmt)
        await session.commit()
