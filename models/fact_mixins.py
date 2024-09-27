from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import mapped_column, Mapped


class PreStageFactMixin:
    unid: Mapped[int] = mapped_column(default=0)
    posting_date: Mapped[datetime] = mapped_column(server_default=func.now())
