from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import mapped_column, Mapped

from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column, Mapped, relationship
from database import Base
from .fact_mixins import PreStageFactMixin


class PreStageMixin:
    checksum: Mapped[str] = mapped_column()
    unid: Mapped[int] = mapped_column(default=0)
    bus_date_from: Mapped[datetime] = mapped_column(server_default=func.now())
