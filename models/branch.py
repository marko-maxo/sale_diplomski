from datetime import datetime
from typing import Optional

from sqlalchemy.orm import mapped_column, Mapped
from database import Base
from .mixins import PreStageMixin, DWHMixin


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
    branch_id: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    date_closed: Mapped[Optional[str]] = mapped_column()


class StageBranch(Base, PreStageMixin):
    __tablename__ = "stage_branch"

    id: Mapped[int] = mapped_column(primary_key=True)
    branch_id: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    date_closed: Mapped[Optional[str]] = mapped_column()


class DWHBranch(Base, PreStageMixin, DWHMixin):
    __tablename__ = "dwh_branch"

    id: Mapped[int] = mapped_column(primary_key=True)
    branch_id: Mapped[int] = mapped_column()
    name: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    date_created: Mapped[datetime] = mapped_column()
    status: Mapped[str] = mapped_column()  # ako je data_closed == null -> 'active' else 'closed'
