from datetime import datetime

from sqlalchemy import TIMESTAMP, func, text
from sqlmodel import Field, SQLModel


class BaseTable(SQLModel):
    """Base table for all models"""

    id: int = Field(description="Auto-incrementing id", primary_key=True)
    created_at: datetime | None = Field(  # type: ignore[call-overload]
        default=None,
        description="Timestamp of creation, defaults to now()",
        sa_type=TIMESTAMP(timezone=True),
        sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP")},
    )
    updated_at: datetime | None = Field(  # type: ignore[call-overload]
        default=None,
        description="Timestamp of last update on record",
        sa_type=TIMESTAMP(timezone=True),
        sa_column_kwargs={"onupdate": func.now(), "nullable": True},
    )
    platform_name: str | None = Field(
        default="Leaflink", description="Platform name (Leaflink/LeafTrade)"
    )