from datetime import datetime
from typing import Optional

from sqlalchemy.orm import mapped_column, Mapped
from database import Base
from .mixins import PreStageMixin


class Customer(Base):
    __tablename__ = "customer"

    id: Mapped[int] = mapped_column(primary_key=True)
    first_name: Mapped[str] = mapped_column()
    last_name: Mapped[str] = mapped_column()
    gender: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    type: Mapped[str] = mapped_column()
    date_created: Mapped[datetime] = mapped_column()
    date_closed: Mapped[Optional[datetime]] = mapped_column()

###

class PreStageCustomer(Base, PreStageMixin):
    __tablename__ = "prestage_customer"

    id: Mapped[int] = mapped_column(primary_key=True)
    first_name: Mapped[str] = mapped_column()
    last_name: Mapped[str] = mapped_column()
    gender: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    country: Mapped[str] = mapped_column()
    type: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    date_closed: Mapped[Optional[str]] = mapped_column()
