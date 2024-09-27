from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column, Mapped, relationship
from database import Base
from .fact_mixins import PreStageFactMixin, DWHFactMixin
from .mixins import PreStageMixin, DWHMixin


class Account(Base):
    __tablename__ = "account"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_type: Mapped[str] = mapped_column()
    date_created: Mapped[datetime] = mapped_column()
    status: Mapped[str] = mapped_column()
    customer_id: Mapped[int] = mapped_column(ForeignKey("customer.id"))

    customer: Mapped['Customer'] = relationship()


class PreStageAccount(Base, PreStageMixin):
    __tablename__ = "prestage_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[str] = mapped_column()
    account_type: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column()
    customer_id: Mapped[str] = mapped_column()


class StageAccount(Base, PreStageMixin):
    __tablename__ = "stage_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[str] = mapped_column()
    account_type: Mapped[str] = mapped_column()
    date_created: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column()
    customer_id: Mapped[str] = mapped_column()


class DWHAccount(Base, PreStageMixin, DWHMixin):
    __tablename__ = "dwh_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column()
    account_type: Mapped[str] = mapped_column()
    date_created: Mapped[datetime] = mapped_column()
    status: Mapped[str] = mapped_column()
    customer_id: Mapped[int] = mapped_column()


class AccountBalance(Base):
    __tablename__ = "account_balance"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("account.id"))
    currency_id: Mapped[int] = mapped_column(ForeignKey("currency.id"))
    balance: Mapped[float] = mapped_column()
    account_balance_date: Mapped[datetime] = mapped_column()

    account: Mapped['Account'] = relationship()
    currency: Mapped['Currency'] = relationship()


class PreStageAccountBalance(Base, PreStageFactMixin):
    __tablename__ = "prestage_account_balance"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_balance_id: Mapped[str] = mapped_column()
    account_id: Mapped[str] = mapped_column()
    currency_id: Mapped[str] = mapped_column()
    balance: Mapped[str] = mapped_column()
    account_balance_date: Mapped[str] = mapped_column()


class StageAccountBalance(Base, PreStageFactMixin):
    __tablename__ = "stage_account_balance"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_balance_id: Mapped[str] = mapped_column()
    account_id: Mapped[str] = mapped_column()
    currency_id: Mapped[str] = mapped_column()
    balance: Mapped[str] = mapped_column()
    account_balance_date: Mapped[str] = mapped_column()


class DWHAccountBalance(Base, PreStageFactMixin, DWHFactMixin):
    __tablename__ = "dwh_account_balance"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_balance_id: Mapped[int] = mapped_column()
    account_id: Mapped[int] = mapped_column()
    currency_id: Mapped[int] = mapped_column()
    balance: Mapped[float] = mapped_column()
    account_balance_date: Mapped[datetime] = mapped_column()
    status: Mapped[str] = mapped_column()  # balance < 0 -> in debt else ok
