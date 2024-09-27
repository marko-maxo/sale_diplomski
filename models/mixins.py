from sqlalchemy import func, text

from datetime import datetime

from sqlalchemy.orm import mapped_column, Mapped


class PreStageMixin:
    checksum: Mapped[str] = mapped_column()
    unid: Mapped[str] = mapped_column(default=0)
    bus_date_from: Mapped[datetime] = mapped_column(server_default=func.now())


class DWHMixin:
    quality_identificator: Mapped[int] = mapped_column(default=0)  # bice 0 ili 1; 0-dobro, 1-error
    bus_date_until: Mapped[datetime] = mapped_column(server_default=text("'9999-07-01'::date"))
