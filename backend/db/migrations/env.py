"""Alembic environment configuration.

The SQLAlchemy URL is derived from DBSettings (backend/db/src/config.py),
which reads DB_* environment variables. This keeps migrations, the ETL,
and the API on a single source of truth for the DB connection.
"""

from alembic import context
from sqlalchemy import engine_from_config, pool
from src.config import DBSettings
from src.models import Base

target_metadata = Base.metadata

# Inject the URL into the alembic config so both offline and online modes
# pick it up without needing a hardcoded entry in alembic.ini.
_db_url = DBSettings().sync_url
context.config.set_main_option("sqlalchemy.url", _db_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    context.configure(
        url=_db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        context.config.get_section(context.config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
