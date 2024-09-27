from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import mapped_column, Mapped


class PreStageFactMixin:
    unid: Mapped[str] = mapped_column(default=0)
    posting_date: Mapped[datetime] = mapped_column(server_default=func.now())


class DWHFactMixin:
    quality_identificator: Mapped[int] = mapped_column()  # bice 0 ili 1; 0-dobro, 1-error