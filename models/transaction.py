from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column, Mapped, relationship
from database import Base
from .fact_mixins import PreStageFactMixin


class Transaction(Base):
    __tablename__ = "transaction"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("account.id"))
    branch_id: Mapped[int] = mapped_column(ForeignKey("branch.id"))
    currency_id: Mapped[int] = mapped_column(ForeignKey("currency.id"))
    amount: Mapped[float] = mapped_column()
    success: Mapped[bool] = mapped_column()
    date: Mapped[datetime] = mapped_column()

    account: Mapped['Account'] = relationship()
    branch: Mapped['Branch'] = relationship()
    currency: Mapped['Currency'] = relationship()

###


class PreStageTransaction(Base, PreStageFactMixin):
    __tablename__ = "prestage_transaction"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[str] = mapped_column()
    branch_id: Mapped[str] = mapped_column()
    currency_id: Mapped[str] = mapped_column()
    amount: Mapped[str] = mapped_column()
    success: Mapped[str] = mapped_column()
    date: Mapped[str] = mapped_column()