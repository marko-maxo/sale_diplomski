from datetime import datetime
from typing import Optional

from sqlalchemy.orm import mapped_column, Mapped
from database import Base
from .mixins import PreStageMixin

class Branch(Base):
    __tablename__ = "branch"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    date_created: Mapped[datetime] = mapped_column()
    date_closed: Mapped[Optional[datetime]] = mapped_column()

###

class PreStageBranch(Base, PreStageMixin):
    __tablename__ = "prestage_branch"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    date_closed: Mapped[Optional[str]] = mapped_column()
