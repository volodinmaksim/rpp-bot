from datetime import datetime

from sqlalchemy import BigInteger, DateTime, func, Text, String, ForeignKey, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class User(Base):
    __tablename__ = "users"

    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    username: Mapped[str] = mapped_column(String(100))
    segment: Mapped[str] = mapped_column(String(15), nullable=True)
    join_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    utm_mark: Mapped[str] = mapped_column(String(100), nullable=True)

    events: Mapped[list["Events"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
    )


class Events(Base):
    __tablename__ = "events"
    __table_args__ = (Index("ix_events_user_id_timestamp", "user_id", "timestamp"),)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"),
        nullable=False,
        index=True,
    )
    event_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )

    user: Mapped["User"] = relationship(back_populates="events")
