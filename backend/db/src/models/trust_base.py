"""Separate SQLAlchemy declarative base for trust-signal tables.

`base_trust_signals` is registered here rather than on :class:`Base` so the KG
loader's ``Base.metadata.drop_all()`` (loader.py) does not wipe accumulated
trust signals on each ``db load``. The trust loader calls
``TrustBase.metadata.create_all(bind=conn)`` idempotently and upserts rows.
"""

from sqlalchemy.orm import DeclarativeBase


class TrustBase(DeclarativeBase):
    """Base class for trust-signal ORM models."""
