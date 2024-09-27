from sqlalchemy.orm import mapped_column, Mapped
from database import Base
from .mixins import PreStageMixin


class Currency(Base):
    __tablename__ = "currency"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    exchange_to_base_currency: Mapped[bool] = mapped_column()

###


class PreStageCurrency(Base, PreStageMixin):
    __tablename__ = "prestage_currency"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    exchange_to_base_currency: Mapped[str] = mapped_column()



