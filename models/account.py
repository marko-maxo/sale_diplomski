from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column, Mapped, relationship
from database import Base
from .mixins import PreStageMixin

class Account(Base):
    __tablename__ = "account"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_type: Mapped[str] = mapped_column()
    date_created: Mapped[datetime] = mapped_column()
    status: Mapped[str] = mapped_column()
    customer_id: Mapped[int] = mapped_column(ForeignKey("customer.id"))

    customer: Mapped['Customer'] = relationship()

class AccountBalance(Base):
    __tablename__ = "account_balance"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("account.id"))
    currency_id: Mapped[int] = mapped_column(ForeignKey("currency.id"))
    account_balance_date: Mapped[datetime] = mapped_column()

    account: Mapped['Account'] = relationship()
    currency: Mapped['Currency'] = relationship()

###

class PreStageAccount(Base, PreStageMixin):
    __tablename__ = "prestage_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_type: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column()
    customer_id: Mapped[str] = mapped_column()

class PreStageAccountBalance(Base, PreStageMixin):
    __tablename__ = "prestage_account_balance"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[str] = mapped_column()
    currency_id: Mapped[str] = mapped_column()
    account_balance_date: Mapped[str] = mapped_column()